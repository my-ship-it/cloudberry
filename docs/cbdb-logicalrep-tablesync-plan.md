# 表同步（初始 + 增表）：现状、实测与后续方案

日期：2026-09-29（原 09-25）。对应分支 `feature/mpp-logicalrep`（64 提交，未推）。

本文只谈表同步这条路：初始同步、`REFRESH PUBLICATION` 增表、以及两者与 apply 循环的
关系。设计总纲见 `cbdb-logical-replication-apply-design.md` §10，工作项见
`cbdb-logical-replication-workitems.md`。

---

## 一、现状

### 1.1 没有 tablesync worker

模块只注册两种 bgworker（`mpp_logicalrep.c`）：

    :1599  MppLauncherMain      一个，常驻
    :1424  MppApplyWorkerMain   每个启用的订阅一个

初始同步和增表同步**都在那个 apply worker 里做**，没有第三种进程。

对照 PG：`process_syncing_tables_for_apply()` 对每张待同步的表
`logicalrep_worker_launch(..., rstate->relid, ...)` 起一个独立进程，每个进程建
**自己的持久槽**（`walrcv_create_slot(..., CRS_USE_SNAPSHOT, &origin_startpos)`，
槽名 `pg_<suboid>_sync_<relid>_<sysid>`），同步完 `ReplicationSlotDropAtPubNode()` 删掉。
并发上限 `max_sync_workers_per_subscription`（默认 2）。

### 1.2 为什么不照搬

PG 的 tablesync 槽同时提供两件东西，我们把它们拆开了：

| 用途 | PG 靠槽 | 我们靠什么 |
|---|---|---|
| 给 COPY 一致的快照 | `CRS_USE_SNAPSHOT`，建槽即导出快照 | 切点：源端 `BEGIN RR` + `(L_t, DS_t, {各段本地快照})` |
| 给追平阶段一条流 | 新槽从 `origin_startpos` 往后读 | 不需要——原有 readers 一直在读、一直缓冲 |

两件都不需要新槽。而在 MPP 里"一个槽"实际是 **N+1 个**（协调器 + 每段），
当前 `max_replication_slots = 10`，3 段集群同步两张表就顶满。加上每个 worker 要自己的
reader/writer gang、以及跨 worker 维持提交序的问题，照搬的代价远大于收益。

### 1.3 初始同步的位置：apply 循环**之前**

```c
LrApplyWorkerRun()
{
    if (mycfg->initial_sync)
    {
        LrStatSetPhase(LR_PHASE_INITIAL_SYNC);
        run_initial_sync(mycfg, tr);        /* 跑完才往下 */
    }
    LrStatSetPhase(LR_PHASE_STREAMING);
    sink = LrMakeCbdbSink(tr);              /* 此时才构造 */
    assembly = LrMakeCbdbAssembly(tr, mycfg, stats);
    lr_apply_driver_run(assembly, sink, NULL);
}
```

`sink` / `assembly` 在初始同步之后才构造，所以**初始同步阶段不存在与 apply 的并发**，
`sync_copy_table()` 用的是紧凑的退避轮询循环。

### 1.4 增表的位置：apply 循环**中间，两批之间**

```c
for (;;)                                       /* 外层 */
{
    assembly->poll(assembly, LR_DRIVER_POLL_MS);
    for (;;)                                   /* 内层 */
    {
        while (assembly->next_unit(assembly, &unit))
            apply_one_unit(...);               /* 当前到期的全部应用完 */
        if (stop || !assembly->housekeeping(assembly))   /* 拷贝推一格 */
            break;
    }
}
```

`housekeeping` → `assembly_housekeeping`（`mpp_apply_worker.c:2523`）→
`driver_sync_progress`（`:1889`）→ `LrSyncAddStart` / `LrSyncAddStep`。

三条约束：绝不打断一个单元；一轮只推一格（不超过一次 libpq 往返）；节奏由 apply 定。

### 1.5 增表的五个交互点

| | 机制 | 方向 |
|---|---|---|
| ① 发现 | `LrSyncPendingNames()` → `d->defer_names` → 每个 APPLY 计划带 `defer=` | apply → sync |
| ② 扣住 | `assembly_advance` 里 `txn_is_deferred()` 命中 → `d->deferred_txns`，不释放 reader 缓冲、`seg_flush` 拉回 | apply 替 sync 保管 |
| ③ 让出 | `LrSyncAddStep` 每次只推一格 | 纪律，非结构保证 |
| ④ 上线闸门 | `stream_applied_through(d, LrSyncAddCutLsn(d->sync))` | sync 等 apply |
| ⑤ 交还 | `catchup_mode` → `unit_build_catchup()` → `catchup=<relids>`，`gxid_covered_by_add_sync()` 去重 | apply 还给 sync |

共享状态全在 `LrDriver` 一个结构里、单线程访问、**没有锁**——这是单进程设计换来的最大简化。

### 1.6 混合事务怎么落地

`assembly->advance()` 在 `apply_one_unit` 的**最后**调（`logicalrep_apply_driver.c:255`），
即单元已提交之后。所以一笔同时触及老表 A 和新表 B 的事务：

1. 段上 `source_emits_rel()` 按 `defer=` 过滤：A 的改动发出、B 的跳过
2. 单元应用 + 提交 ← **A 的改动此刻已落库**
3. `advance()` 发现它触及 B → 进 `deferred_txns`，只是不释放
4. catchup 只发 B（`only_relids` + `filter_only=true`），A 不会再来一遍

**"扣住"扣的是 reader 缓冲和位点，不是事务的应用。**代价是原子性被拆成两笔——PG 同理
（`should_apply_changes_for_rel` 也让两个 worker 各应用一部分）。

### 1.7 全量与增量的边界

```
        增表切点                              表上线
           │                                    │
───────────┼────────────────────────────────────┼──────────►
    ①      │              ②                     │     ③
在 COPY 里  │   不在 COPY、被扣住                  │   正常流式
```

| 边界 | 谁保证 |
|---|---|
| ①/② | `gxid_covered_by_add_sync()`（`mpp_apply_worker.c:2092`），判据 `visible(G, DS_t)` |
| ②/③ | 上线闸门——切点前提交但还没流到 apply 的事务必须先被扣住才能放行（R12 抓出的真 bug） |
| 扣住的完整性 | `txn_is_deferred()` 对每笔事务判一次 |

### 1.8 COPY 命令什么时候发给源端

在 `LR_SYNC_STEP_PORTS` 状态下、**确认目标端所有 helper 端口都已发布之后**：

```c
if (s->step == LR_SYNC_STEP_PORTS)
{
    if (copy_ports_ready(s, r))     /* 拼出 s->cur_targets */
    {
        copy_send(s, r);            /* ← 这时候才发 */
        s->step = LR_SYNC_STEP_COPY;
    }
}
```

因为源端语句的 PROGRAM 串里要嵌 `--targets <host:port,...>`，而端口是目标端的接收语句
（`INSERT … gp_lr_copy_recv(…)`）触发各段 fork helper 后才有的。顺序不可换。

---

## 二、实测数据

### 2.1 初始同步：握手占比 ≈ 100%

50 张表一个 publication，3 段集群：

| 每表行数 | 退避改动前 | 改动后（`fd7885ca47a`） |
|---|---|---|
| 1 | 15.2 s | **7.6 s** |
| 100 | 16.1 s | **5.7 s** |
| 10000 | 15.5 s | **7.3 s** |

**行数放大一万倍，总耗时不变**——时间没花在搬数据上。逐表间隔中位 **251ms → 74ms**。

根因：`copy_ports_ready` / `copy_poll_done` 都用固定 `LR_SYNC_POLL_MS = 100`
（编译期 #define），小表两次等待 = 200ms。改成 1ms 起翻倍到 100ms 封顶。

### 2.2 剩下 74ms 的去向（临时埋点量的，已撤）

| 阶段 | 中位 | 干什么 |
|---|---|---|
| `begin` | 6.0 ms | 状态行 UPDATE + 下发接收 INSERT |
| **`ports`** | **24.7 ms** | 各段 fork helper、bind 端口、写 port 文件 |
| `send` | 0.0 ms | `PQsendQuery` 异步，不等 |
| **`done`** | **28.3 ms** | 等两端完成 |

`ports + done = 90%`。`ports` 是真实工作（fork+exec+bind），压不动。
`done` 里约一半仍是退避台阶，可改成在 socket 上等。

### 2.3 增表的节奏（**原文的"三秒"是错的，已实测更正**）

`LrSyncAddStep` 不走 `sync_wait`，靠 apply 主循环调度：`driver_sync_progress` 在拷贝进行中
返回 **false** → 内层循环跳出 → `assembly->poll(...)`。每表两态，所以**一次 poll 的等待
就是一格的时长**。

原文据 `LR_DRIVER_POLL_MS = 1000` 推出"四态机三格就是三秒"——**推错了**。
`assembly_poll` 自己先压过一道：

```c
int timeout = gp_lr_batch_max_latency_ms;   /* 200 */
timeout = Min(timeout, timeout_ms);          /* Min(200, 1000) */
```

所以真实节奏一直是 **200 ms**，不是 1 秒。实测基线（10 张小表一次 REFRESH）：

| | 每表 | 10 表总计 |
|---|---|---|
| 修改前 | **0.726 s** | 7.26 s |
| 只压 poll 超时 | 0.412–0.498 s | 4.1–5.0 s |
| **再加"有进展就重置退避"**（P1 落地版） | **0.332–0.344 s** | 3.3–3.4 s |
| 对比：初始同步同样的活（§2.1） | 0.074 s | — |

剩下的 4.5 倍差距是每格自身的往返，属于 P3 / P4 的地盘。

### 2.4 快照能否共享（决定并行的形态）

| | 分布式快照 | 各段本地快照 |
|---|---|---|
| `pg_export_snapshot()` + `SET TRANSACTION SNAPSHOT` | **能共享**（实测 xmax 一致） | **不能**（实测 B 与未导入的 C 相同，A 不同） |

所以 K 个并行会话**必须各取各的切点**，共享切点这条路不通。
→ **"每表一个切点"是并行的前置条件，不是可选项。**

### 2.5 资源上限（3 段集群实测）

| 资源 | 上限 | K=4 时用掉 |
|---|---|---|
| 端口范围 `copy_port_lo..hi` | 42000–42999，1000 个 | 4×M = 12 |
| 段 `max_connections` | 750 | 4×(N+M) 套 QE |
| `max_replication_slots` | 10 | **0 新增** |
| `max_worker_processes` | 32 | **0 新增** |
| `mpp_logicalrep.max_workers` | 8 | 0 新增 |

---

## 三、待办，按优先级

### P0 — local 事务在增表路径上丢行 —— **已修复**（`82338965883`，2026-09-23）

**曾经的问题**：utility 模式直连段的写入没有 gxid。`assembly_advance` 里
`txn_is_deferred()` 只对 `batch_gxids` 调用，`batch_locals` 无条件进
`release_locals`。于是一笔落在正在增的表上的 local 写入：

1. 改动被 `defer=` 跳过（`source_emits_rel()` 纯按 relid，不看事务类型）
2. 事务照样被释放，reader 缓冲没了、位点推进
3. 从没进 `deferred_txns`，catchup 不会重放它

→ **永久丢失**。窗口 = 增表切点 → 表上线。实测：源 2000001 行 / 目标 2000000 行。

**改法**：

1. locals 循环加 `txn_is_deferred(d, src->rel_names)`；local 版 `LrDeferredTxn`
   用 `gxid == 0` 区分，靠 `seg` + `event_lsn`（= 该段位点）定位，不需要 `seg_lsn[]`
2. `unit_progress` 让这一笔把**它那一个段**的 flush 压住（漏了这步崩溃重启会丢）
3. `unit_build_catchup` 按单参与段发出；新 `deferred_release()` 按类型选
   `release_gxids` 还是 `release_locals`
4. 去重：`LrSyncAddSnapshot()` 顺带取回切点的 `local=`（`sync_parse_local()` 从
   `sync_apply_snapshot` 抽出来共用），驱动解析成每段 `LrLocalSnap`，
   `local_covered_by_add_sync()` 与分布式那条并列。reader 在 LOCAL 报告的身份字段里
   带上自己的 xid——那个字段对 local 事务本来是空的（STATUS 行早就在复用它了）

**没走的那条路**：把增表切点的 `local=` 下发给已连接的 reader、让 reader 自己过滤。
判据留在驱动侧才与 gxid 那半对称，也不用新开下发通道。

**用例**：`isolation2/add_utility_writes`，两半都覆盖——切点之后写（copy 里没有，
catchup 必须重放）、切点之前写（copy 里有，catchup 必须丢掉），各自恰好一行。
反向验证过：把 locals 的 defer 判断去掉，`replayed_once` 从 1 变 0。

### P0.1 — 集群重启会把订阅永久停掉 —— **已修复**（`65e83f0227b`，2026-09-23）

**曾经的现象**：`gpstop` + `gpstart` 之后订阅变成 `enabled = f`，
`disable_reason = slot "ss_slot" is usable on only 2 of 4 source nodes`。
段全起来之后手动 ENABLE 立刻追平，**4 个槽一个没少、全 `reserved`**。

**根因不是槽，是跑错了协调器**。gpstart 先用 `-c gp_role=utility` 起一个协调器做自己的
引导（`gppylib/commands/gp.py` 的 `set_utility()`），跑完就停掉、再以正常模式重起。
launcher 只判 `IS_QUERY_DISPATCHER()`——那是按 content id 判的，utility 协调器一样为真——
于是 worker 在那个协调器里起来，没有 gang，把源端读成"大部分槽丢了"，
把订阅停掉，然后自己 300ms 后被杀。实测时间线（demo 集群）：

```
+0.00s  MPP apply worker for subscription "s" started
+0.08s  ERROR  usable on only 2 of 4 source nodes  → 停用订阅
+0.17s  同一个协调器上探针读到:  2
+0.38s  launcher: terminating ... due to administrator command  ← 这个 postmaster 正在被停
+1.58s  新协调器上读到:  4
```

**改法**：`Gp_role != GP_ROLE_DISPATCH` 也不跑。`gp_role` 对一个 postmaster 是终身固定的，
所以这是**闸门不是等待**；utility 模式下这个 worker 就算等也干不了任何事。
已核实 `Gp_role` 对普通 bgworker 不会被重置（`postinit.c` 只对 retrieve 连接改，
`autovacuum.c` 是自己强设）。

**没改判据**：`ERRCODE_OBJECT_IN_USE` 仍然是永久错。launcher 退出那个协调器之后，
"数不齐"就真的意味着某个节点丢了槽（段 failover 到没有槽的 mirror），重启多少次都一样；
而源端段**真的下线**时，`gp_replication_slots` 这条查询会直接失败（建不了 gang），
那个错不在永久类里，worker 退出、launcher 重启它——已经是对的。

**用例**：`test/cluster/restart.sh`（双集群），三段：smart gpstop 能不能结束、
gpstart 后订阅是否自己回来并追平、fast 停再起是否一样。

**没走的那条路**：轮询 `gp_segment_configuration` 等段全 `status='u'`。
实测到的全部场景都被上面这道闸门覆盖了，轮询是防守纵深，不是根因。

### P0.5 — reader 侧无界积累 —— **已修复**（`16b764682d6`，2026-09-24）

**曾经的问题**：copy 期间被 defer 的事务，**行留在 reader 里**——`LrDeferredTxn`
只存坐标，真正的变更靠不发 release 押在各段 reader 的内存和 spill 文件里。
押多久 = copy 时长，押多少 = 源端写入速率 × 它。`gp_lr_reader_mem` 不是上限，
是**内存与磁盘的分界线**；真正的硬边界是段磁盘，撑爆它没有旋钮、没有预案。

**PG 没有可抄的东西**：PG 的 tablesync worker 拿自己的槽、copy 完自己接着流式追平，
主 apply worker 对该表的变更直接丢弃（`worker.c:2410`，`return` 之后事务照常提交、
origin 照常推进）。PG 用"重复解码"换掉了"无界缓存"，所以它根本没有需要反压的对象。
它唯一的同类成本是 sync 槽钉住 `restart_lsn`，对策是熔断（`max_slot_wal_keep_size`），
我们已有对等物（O1）。

**机制**：不是限速器，是 TCP 流控。唯一的动作是**不调 `recv`**：

```
driver: max(held) ≥ hold_max
  └─ DRAIN 带 wait_ms < 0
       └─ reader 不进 LrReaderReceive()
            └─ 内核缓冲填满 → TCP 零窗口
                 └─ 源端 walsender 阻塞在 ProcessPendingWrites() 的 WalSndWait(WRITEABLE)
                      └─ 不再解码 → restart_lsn 钉住 → WAL 留在源端
```

压力落点从"目标段磁盘（无旋钮无预案）"挪到"源端 WAL（有 `max_slot_wal_keep_size`、
有 O1 恢复路径）"。**总量没变，落点变了**，这就是全部收益。

**三条非做不可的配套**（每条漏掉都会变成另一个 bug）：

1. **feedback 不能跟着停。** 被堵住的 walsender 仍在 `ProcessRepliesIfAny()` 读我们的回复、
   仍按 `last_reply_timestamp` 计 `wal_sender_timeout`（`walsender.c:1450, 2494`）。
   停了 feedback → 60 秒后连接被杀 → 重连 → **押着的那堆全部重新解码传一遍** →
   越暂停越涨，周期 60 秒。DRAIN 第 3 步无条件执行就是为这个。
2. **等待要挪到驱动侧。** 平时是 reader 在 `LrReaderReceive` 里替驱动等；不读了就没人等，
   驱动会热自旋。`driver_drain` 末尾补 `driver_wait(held_ms)`。
3. **只算"已完成"的事务。** 一笔还在到达的事务谁也释放不了，停掉它自己 COMMIT 所等的读
   = 永久卡死。所以 reader 报的从 `mem_bytes + spill_bytes` 改成
   `held_bytes`（非 OPEN 事务的 `changes->total_len` 之和）。

**一条非关掉不可的例外**：同步作业的流还没追到切点时**不许暂停**。表上线要等
`stream_applied_through(L_t)`，而追到切点的唯一办法是读——在那里暂停会挡住
"唯一能释放积压的 catchup"，**这是死锁不是慢**。代价有界:切点在作业开始时就定死了，
追到它只需吞掉那一刻的 backlog，而不是 copy 的整个时长(后者才是无界增长那部分,
过了切点照样压)。

**其余判据**：取 `max(seg_buffered_bytes)` 不取 sum（撑爆的是某一个段的磁盘）；
全局生效（分布式事务要所有参与段报齐才 ready，只停胖的那段会缓冲永远凑不齐的分片）；
滞回到 `hold_max/2` 恢复（否则阈值附近每个 poll 翻一次）。

**GUC**：`mpp_logicalrep.hold_max`（kB，默认 1 GB，0 = 关，`PGC_USERSET` 所以
`ALTER DATABASE ... SET` 送得进 worker）。**没加视图列**——那要改
`gp_stat_get_mpp_subscription()` 的 tupledesc + 视图定义 = 目录变更；
现用两条 LOG + 现成的黑盒证据（段的 `pos` 冻住）。

**用例** `isolation2/backpressure`：窗口是**造出来的不是抢出来的**——源端 bp_b 上持
AccessExclusive 卡住它的 copy，而作业里任何一张表没拷完就不算完，所以 bp_c 一直被 defer；
写 bp_c（没人锁它）。断言:源端写 20000 行而各段 `pos` 三秒不动;放锁后每行恰好一次。

**踩到的坑**：① 第一版用"先写 backlog 再增表"，实测 copy 只要 2.3 秒而追切点花了 4 秒，
暂停到解压只隔 **16 毫秒**——窗口结构性地落在 copy 之后。② `REFRESH PUBLICATION` 会向发布端
问表，那会读表，所以锁必须在 REFRESH **之后**才能加（否则 REFRESH 自己被锁住），
于是要 DISABLE → REFRESH → 加锁 → ENABLE。③ isolation2 怕 `::`，要写 `cast(x as text)`。

**余下**：视图加 `paused` 列（需目录变更）；`LR_STAT_MAX_SEGS = 128` 以外的段判据看不见。

### LOCK1 — GDD 关闭时 apply 的锁级别 —— **已修**（`ead71a321f0`，2026-09-25）

**用户提出**。Cloudberry 在 `gp_enable_global_deadlock_detector` 关闭时
（**这是默认**）把 UPDATE/DELETE 的 `RowExclusiveLock` 升级为 `ExclusiveLock`
（`CdbTryOpenTable`，`src/backend/access/table/table.c:149`）——没有检测器时，
跨段铺开的行等待环**没人破**：每个段都看不到本地环，就永远等下去。

**apply 走不到那条路**：它用 `LOCK TABLE` 加锁，那是 `LockTableCommand`，
要什么给什么，**不经过 `CdbTryOpenTable`**；而它对除 TRUNCATE 以外的一切
都只要 `RowExclusiveLock`。实测这台（GDD=off）：普通 `UPDATE` 拿 `ExclusiveLock`，
apply 拿 `RowExclusiveLock`——**apply 成了唯一一个在 RowExclusiveLock 下持有跨段行锁的会话**，
正是那条规则要堵的洞。

**改法**：reader 按"假设没有检测器"报告锁级别（TRUNCATE → `AccessExclusiveLock`，
动过 UPDATE/DELETE → `ExclusiveLock`，只 INSERT → `RowExclusiveLock`；
新增 `LrTxn.has_rowlock`）；**有没有检测器是协调器那个集群的事、不是这条流的事**，
所以 `lock_for_policy()` 在驱动侧决定，开着时再降回去。

**加锁顺序本来就是对的**（同一次提问顺带查证）：`unit_add_rel` 按变更到达顺序追加，
但 sink 在发第一条 `LOCK TABLE` **之前** `qsort` 按**目标表 OID** 排序
（`mpp_sink_cbdb.c:255`, `cmp_sink_lock`），所以两个 apply 单元之间、
apply 与普通语句之间永远按同一个全序拿锁；`LOCK TABLE` 又会
`CdbDispatchUtilityStatement` 分发到段（`lockcmds.c:80`），
段上的表锁在任何 writer 动手之前就位，之后的到达顺序只涉及行锁。

**未覆盖**：检测器**开着**时，同一条规则对 **AO/AOCO 目标表**的 UPDATE/DELETE 同样升级，
而目标表的存取方法驱动现在不知道（`sync_fill_source_info` 取的是**源端**的 AM）。
补法很小：`sync_fill_columns_all` 那条查询多取一列目标端 `pg_am.amname`。

### LOSS1 — 大事务被当成"什么都没写"整笔丢掉 —— **已修**（2026-09-28）

**用户压测发现**（"一次插入 1000 万条数据，看多久同步到目标端"）。现象不是慢，是**静默丢数据**：
源端 `INSERT ... generate_series(1, 10000000)` 一笔提交，目标端**一行都没到**，
而且订阅照常 `streaming`、`stuck_errors = 0`，此后的小事务还能继续同步。
590MB reader 缓冲**永久挂住**（三天没释放）。

**复现（跨集群 :7100 三段 → :7300 两段，单事务 INSERT）**

| 行数 | 默认宽限期(10s) | 宽限期调到 600s |
|---|---|---|
| 100k | 秒级到达 | — |
| 500k | 提交后 12.8s 到达（刚好踩线） | — |
| 2M | **240s 未到，永久丢** | 提交后 37.7s，完整 |
| 10M | **永久丢** | — |

**根因**。协调器从自己那条流拿到分布式提交事件，然后等参与段把各自那份送上来。
一个"什么都没写（这条订阅关心的东西）"的段**永远不会送**，所以这个等待不能无限：
`txn_complete()` 等满 `event_wait_ms`（`exact` 列表 ×10 = 默认 10s）就放弃。
放弃之后 `driver_collect_ready()` 看到 `nready == 0`，判定
"complete but reported by nobody → 它没写任何我们复制的东西"，
**把整笔事务从注册表里删掉并推进提交位点**——于是那 1000 万行既不会到，也不会再被提起。

这个推断只有一种情况是错的，恰好就是最贵的那种：**段还在发**。
一笔大事务在线上传的时间远超任何合理的默认宽限期。
日志里那条 `gxid N applied without segment 0 ... (0 of 3 participants reported)`
就是它，但 WARNING 的措辞让人以为只是"少了一个段"，实际是整笔没了。

**改法：让等待有证据，而不是有时限。**
reader 每次 DRAIN 额外汇报自己**正在接收**的事务（新 `LR_REPORT_OPEN 'O'`，
`LrReaderOpenGxids()`）；驱动侧 `LrSegPart.open` + `LrDriver.seg_open[]` 记下来，
`txn_complete()` 在段"还在读"时**不启动宽限期**，而是把 `first_wait` 推到它安静下来那一刻。

两处细节：

- **gxid 为 0 也要报**。大事务会被源端 streaming 出来，`BEGIN PREPARE` 里那个
  带 gxid 的 gid **要到最后的 PREPARE 才到**，所以正在传的大事务在 reader 那里
  **还没有名字**。只按 gxid 报（第一版就是这么写的）对这个 bug **完全无效**——
  实测第一版仍然丢。改成"有名字报名字、没名字报 0"，0 表示"这个 reader 忙着"，
  而一个忙着的 reader 还没读到它后面的东西。
- **"忙"这种模糊证据只对 `exact` 列表算数**。`exact=1` 的列表是
  "PREPARE 时报告写过 WAL 的段"（`XACT_XINFO_DISTRIB_SEGS_EXACT`，`xact.c:7135`），
  那么它迟早会送到，等是对的；`exact=0` 是"分发到的段"，其中有的本来就没东西送，
  不能让一个忙碌的邻居把所有人卡住。**精确证据**（这笔事务就在那个段上开着）
  两种列表都算数。

顺带修的两个：

- **缓冲泄漏**：走"没人报告→丢掉"那条路时把 gxid 也发一遍 `release_gxid`。
  之前那条路认定"没人持有"，可实测三天挂着 590MB——reader 可能在最后一次 DRAIN
  之后才收完。reader 对不认识的 gxid 本来就忽略。
- **别释放还开着的事务**：`drain_one` 的释放循环加 `txn->state != LR_TXN_OPEN`。
  正在接收的事务被释放，剩下的流就没地方落了。

**验证**。修复前后各跑同一个 2M 单事务用例，集群整体重启保证段上 `.so` 也换掉：
**旧版 2/2 丢，新版 3/3 完整**；清空重建基线后 2M + 小事务，两端
`count` 与 `sum(hashtext(...))` **完全一致**。用例 `isolation2/slow_arrival`
把宽限期压到 100ms（×10 = 1s）、发一笔 50 万行的单事务，几秒的传输就能越过它。

**"空事务会不会被 pgoutput 跳掉"——查证后更正（2026-09-29，用户提问带出来）。**
原先这里写的"`exact=1` 的段如果写的全是不在发布里的表，pgoutput 跳过空事务、它也永远不报"
**是错的**，实测和代码都不支持：

- `skip-empty-xacts` 是 **test_decoding 的选项，pgoutput 没有**。pgoutput 的跳过是 PG15 起的
  无条件行为：`pgoutput_begin_txn` 只分配 `PGOutputTxnData` 不发 BEGIN，第一条变更时才
  `pgoutput_send_begin()`，`pgoutput_commit_txn` 看 `sent_begin_txn` 为假就整笔跳过。
- **一阶段**：CBDB 已经改过——`pgoutput_commit_txn` 里
  `if (data->cbdb_gxid && txn->distribXid != 0 && !txndata->sent_begin_txn) pgoutput_send_begin()`，
  即分布式事务哪怕是空的也补发 BEGIN + `cbdb.gxid` + COMMIT。reader 连接时下发
  `cbdb_gxid 'on'`（`mpp_reader.c:424`），所以这条对本模块始终生效。
- **两阶段**：`pgoutput_begin_prepare_txn` / `pgoutput_prepare_txn` **根本没有跳过逻辑**，无条件写，
  `BEGIN PREPARE` 里带的 gid 就是 gxid。

真正"什么都不发"的位置更上游，在 reorderbuffer：`ReorderBufferPrepare`/`Commit` 里
`ReorderBufferTXNByXid(create=false)` 返回 NULL，或 `ReorderBufferReplay` 里
`txn->base_snapshot == NULL` 就提前 return，回调根本进不去。

**实测（release 集群 :7500→:7700，发布只含 t1,t2）**：

| 事务只写 | 结果 |
|---|---|
| 未发布的 heap 表 `t3` | 段照报，随后的 marker **2.0s** 到达，无 warning；日志 `batch 4 committed: 3 gxid txns` |
| **AO 表 `t4`（`USING ao_row`）** | 同样照报，marker **1.2s** 到达，无 warning；`batch 5 committed: 3 gxid txns` |

AO 也能报的原因：`appendonly_decode()` 虽是空桩、`RM_SEQ_ID` 的 decode 回调干脆是 NULL，
但 AO 写入同时会动它的辅助 heap 表（aoseg/visimap），那是可解码的，事务因此有 `base_snapshot`。
**判据**：apply 是严格提交序，若那笔没被报告，后面的 marker 必须等满 10s 宽限期才能应用；
1–2s 到达即证明三个段都报了。

**所以剩下的窗口很窄**：要一笔分布式事务写了 WAL（才会进 `exact` 列表）却完全没有可解码记录。
已知的真实例子是**建槽边界**（段侧 PREPARE 早于段槽起点，coordinator 侧的事件却在槽之后），
那条本来就由 `seg_moved_past` 兜住。AO/AOCO 作为**源端**表的整体支持仍是单独立项（§A）。

### PERF1 — apply 每行取一次事务快照 —— **已修**（`def65108017`，2026-09-28）

**用户追问"追平 40 秒慢在哪"带出来的。** 用 perf + strace 把一笔 2M 行单事务拆开：

| 阶段 | cassert 构建 | 证据 |
|---|---|---|
| 传输（源端解码+发送 → 三段 reader 收齐） | 13.6s | `segbuf` 一直 0(只统计已完成事务)、`pos` 在推进 |
| apply | 28.9s | `batch_txns=1` 的持续时间 |

apply 期间 296 次等待事件采样:**290 次在 CPU**、5 次 Interconnect、1 次 IO——纯 CPU-bound。
strace 抓到 **10 秒内 47,320 次 `openat`**,全是同一个 `/dev/shm/PostgreSQL.<handle>`,
配套等量 `mmap`/`fstat`/`close`/`munmap`。`strace -k` 的栈:

    lr_apply_next → GetTransactionSnapshot → GetSnapshotData
      → updateSharedLocalSnapshot → dsm_attach(shm_open+fstat+mmap)
                                  → dsm_detach(munmap+close)

**链条**:① `mpp_writer.c` 的 `begin_step()` **每条变更**做一次
`PushActiveSnapshot(GetTransactionSnapshot())`(照搬上游 `worker.c` 的 `begin_replication_step`);
② apply 事务是默认 **READ COMMITTED**,`GetTransactionSnapshot()` 每次都真调 `GetSnapshotData()`;
③ CBDB 在 `GetSnapshotData()` 里对 `QE_*_WRITER` 上下文一律调 `updateSharedLocalSnapshot()`
(`procarray.c:3438`);④ 那个函数**每次都 `dsm_attach` → 序列化快照 → `dsm_detach`**
(`procarray.c:2292`)——注意它创建时刚 `dsm_pin_mapping()` 过,末尾又无条件 detach 掉,pin 被自己抵消。
**这条路所有 Cloudberry writer QE 都在走**,不只逻辑复制。

**改法(插件侧)**:一条变更真正需要快照的,只是"看见同批次里更早那些行"——那是 **command id**,不是新快照。
`end_step()` 本来就 `CommandCounterIncrement()`,配上 `UpdateActiveSnapshotCommandId()` 即可。
于是整批在第一步取一次快照、`finish_batch()` 放掉;AO 路径里原本"换一张新快照"的地方改成推进 command id。
**放弃的是**:看见批次中途提交的本地事务。目标端的表本来就只该由 apply 写,有别的写者本身就是冲突。

**也试过内核侧**(用户先选的):在 `updateSharedLocalSnapshot()` 里按 handle 缓存 `dsm_segment`、
`AtEOXact_SharedSnapshot()` 里释放。apply 同样 28.9s→5s,但**那版有 bug**
(`dsm_attach` 对已 attach 的段会报 `can't attach the same segment more than once`),
且改的是内核公共路径;用户随后决定只改插件。**内核那条仍是个真问题,值得单独立项**。

**真实基线(release 构建,`--disable-cassert`,:7500 三段 → :7700 两段,同一套集群同一份数据 A/B)**

| 2M 行单事务 | 修复前 | 修复后 |
|---|---|---|
| 传输 | 8.9 / 9.3 / 9.5s | 9.4 / 9.6 / 9.8s |
| **apply** | **25.8s** | **4.1s** |
| 追平(提交后) | 34.7 / 36.0 / 35.6s | **13.5 / 13.8 / 13.9s** |

目标端原生 `INSERT ... generate_series` 2M 行(同结构带主键)= **3.4s**,
即 **apply 只比本地插入慢两成**。10M 行单事务:追平 **61.1s**(传输 43.3 + apply 17.8)。

**cassert 构建的数字不能当生产数字**(同一修复在 cassert 上是 40s→15~19s)。
perf 显示 `--enable-cassert` 自动打开的三个宏吃掉大头:
`RELCACHE_FORCE_RELEASE` 让源端 walsender **每条变更重建一次 relcache**
(`ReorderBufferProcessTXN → RelationIdGetRelation → RelationBuildDesc` 占该进程 **63.8%**,
而真正干活的 `pgoutput_change` 只有 6.3%)、`CLOBBER_FREED_MEMORY` 的 `wipe_mem`(28.4%)、
`MEMORY_CONTEXT_CHECKING` 的 `AllocSetCheck`(目标端 reader 30.8%、apply 32%)。

**剩下的大头是传输**(2M 约 9.5s,占总时长七成),下一个优化目标在那边。

### DB1 — 一阶段 marker 不带库，跨库泄漏 —— **已修**（`cb6c726f7ef`，2026-09-29）

**用户追问"一阶段提交 apply worker 一定会收到通知吗"时查出来的。**

两阶段的事件是协调器的 commit record，**原生就带 `parsed.dbId`**，解码时有过滤：

    if (parsed.dbId == ctx->slot->data.database && ...)

而一阶段的 `XLOG_XACT_DISTRIBUTED_ONEPHASE` 是**我们自己加的记录**（main 里没有），
当初只放了 `gxid` + `seg`，**没有库**——因为协调器在一阶段里自己没写任何东西，
没有 commit record 可以搭。于是解码时**没法过滤**，集群上**任何一个库**的一阶段提交
都会被 announce 给所有槽。

**后果**：那笔事务的段侧数据在别的库，本槽的段流里永远不会有 → 驱动等满宽限期 →
而 apply 是严格提交序，**后面所有事务一起堵**。多库活跃的集群上，别的库每提交一笔
单段事务就付一次 10 秒。**isolation2 整套测试就是同集群形态**（源库 `mpp_lr_source`、
目标库 `mpp_lr_*`），正好是重灾区。

**改法**：`xl_xact_distributed_onephase` 加 `Oid dbId`（写的时候 `MyDatabaseId` 现成），
解码时照抄两阶段那行过滤。记录还没进主干，改格式没有兼容负担。

**踩的两个坑**：

1. **`pg_waldump` 必须一起重编**。它链的是自己那份 `xactdesc.c`，只 `make -C src/backend`
   的话它还按旧结构体解析，`seg` 读成乱码（`seg -23240`）。要 `make -C src`。
2. **第一版用例测不出来**。`seg_moved_past` 会在"后面那笔 mark 被同一个段报告"时
   把外库那笔划掉——如果两者恰好落同一个段，等不满宽限期，A/B 都是 `t`。
   改成在外库做 **30 笔单行一阶段提交铺满所有段**（`other_spread()`），
   mark 只落一个段，其余段上的外库 gxid 就划不掉了。
   **A/B 干净**：无过滤 `f`（用例 10.7s），有过滤 `t`（5.1s）。
   断言用 5 秒预算的 `lr_wait_5s`——宽限期是 10×`event_wait_ms`=10s，5 秒能把两种行为分开
   （原来的 `lr_wait` 是 180 秒预算，**有 bug 也会通过，只是慢**）。

**全套 30 绿 + pg_regress 2 绿 + 单测 9/9。**

### EMPTY1 — 空参与段永不出声 —— **已修**（`64da2304466`，2026-09-29）

**用户追着问出来的。** 两阶段事务里，一个**被分发到、prepare 了、但没写任何可解码记录**的段
永远不 announce，驱动只能等满宽限期。实测这不是边角情况：
**单键 `UPDATE … WHERE id=2` 分发给全部三个段，三个都写 PREPARE 记录，只有一个写堆更新**——
空参与段是常态。触发场景：UNLOGGED 表、**临时表 ETL**（`INSERT INTO tmp` + `UPDATE 被复制的表`
是极常见写法）、行锁（GDD 开时）。每笔付 **11.6s**，且严格提交序把后面全堵住。

**根因链条**(逐层查证)：
1. `base_snapshot` 只由**变更解码器**建(`SnapBuildProcessChange`,注释:*"it will be needed to
   decode the change we're currently processing"*)或目录变更事务在 `SnapBuildCommitTxn` 补建；
   xact 解码器(`DecodePrepare`/`DecodeCommit`)**假定事务已存在**
2. 于是空参与段:`ReorderBufferTXNByXid(create=false)` 返回 NULL → `RememberPrepareInfo` 返回
   false → `DecodePrepare` 直接 return；即使有 txn 也会在 `ReorderBufferReplay` 的
   `base_snapshot == NULL` 提前 return
3. **PREPARE 记录和 GID(= 十进制 gxid)本来就在 WAL 里**,只是解码时被丢掉
4. 单机 PG 里"没 base_snapshot ⇒ 订阅端不需要知道"完全成立;MPP 里不成立——
   **"这个段没东西给你"和"这个段不用等"是两回事**

**改法(第三版,最终)**:在 `DecodePrepare()` 里补调一次 `SnapBuildProcessChange()`——
**那正是非空事务解码第一条变更时走的同一条路**。它内部的 `ReorderBufferSetBaseSnapshot()`
自己会 `ReorderBufferTXNByXid(..., create=true, ...)` 建事务,于是一次调用同时给了事务和快照,
之后 `RememberPrepareInfo → Replay → ProcessTXN(begin_prepare + 零变更 + prepare) →
FinishPrepared(commit_prepared)` **全走原生**。
内核净改动:`reorderbuffer.h` 一个 bool、`reorderbuffer.c` 一行初始化、`decode.c` 四行、
`pgoutput.c` 一行(由 `cbdb_gxid` 选项门控,别的消费者行为不变)。
**相对上游 main,`reorderbuffer.c` 是纯新增 17 行、零删除。**

**走过的两条弯路**(都实测炸了,值得记):
- **第一版(手建空壳 txn + 手调回调)**:踩 `ReorderBufferFinishPrepared` 里的
  `Assert(txn->final_lsn != InvalidXLogRecPtr)`——PG 的不变式是"能走到这儿的事务必然经历过
  `RememberPrepareInfo`,所以 final_lsn 一定有",我们凭空建的事务破坏了它。
  段 walsender 反复崩溃 → reader 反复重连 → 延迟 44s → **190 个 core 把磁盘再次写满**。
  `ReorderBufferRestoreChanges` / `ReorderBufferRestoreCleanup` 里还有同类断言,spill 路径会再踩。
- **第二版(挪到 commit 侧)**:用户要求"不要 prepare 逻辑"。同一个洞在
  `ReorderBufferFinishPrepared` 开头一样在(`txn == NULL` 直接 return);而且在 COMMIT PREPARED
  时建的事务没有 prepare 信息,要么撞断言,要么把 `final_lsn` 设成 commit 位置导致
  `(final_lsn < two_phase_at)` 为假、**跳过重放只发 commit_prepared**,reader 状态机接不住,
  要让重放触发就得**伪造一个小于 `two_phase_at` 的 LSN**。所以 commit 侧无法原生。
  (顺带:实测 commit 版延迟和 prepare 版一样 1.3s,我先前"每笔多一轮广播"的预测没测出来。)

**验证**:纯 heap 基线 / UNLOGGED 混写 / 临时表 ETL 全部 **1.3–1.4s**（此前 11.6s），零新增 warning。
全套 **30 绿 + pg_regress 2 绿 + 单测 9/9**。

**Cloudberry 上断言失败的排查方式**（这次踩到）：`ExceptionalCondition()` 被改成先
`ereport(FATAL, ..., errmsg("Unexpected internal error"), errdetail("(\"%s\", File: \"%s\", Line: %d)"))`
再 `abort()`，理由是 MPP 要让 QD/客户端知道发生了什么。后果是**目标端只看到
`FATAL: Unexpected internal error (assert.c:46)`——那是 ereport 自己的行号，不是失败的断言**；
真正的条件/文件/行号在 `errdetail` 里，而 errdetail 不跟着 WAL 流传过来，**必须去源端段日志捞**。

### DELWROTE — 删掉 QE→QD 的"写了没有"回报 —— **已做**（`0f962f8d3c4`，2026-09-29）

EMPTY1 之后，被分发到的段 = 会 announce 的段，S4b 那套精确化失去意义，而且它的判据
（`TransactionIdIsValid(GetTopTransactionIdIfAny())`，"拿到 xid"）本来就不等于"产生了可解码输出"。

删掉：`XACT_XINFO_DISTRIB_SEGS_EXACT` 标志位、`dtxWriteSegments`/`dtxWriteSegmentsKnown`、
`cdbtm.c` 里收集回报那 35 行、`DtxPrepareWrote()`/`dtx_prepare_wrote`、
`postgres.c` 里 `qc.nprocessed = DtxPrepareWrote() ? 1 : 0` 这条协议、
`decode.c`/`pgoutput.c` 里 `exact` 的层层传递、dcommit 消息里的 `exact=` 词、
插件侧 `segs_exact` 与两套宽限期规则。共 **-155 / +56 行**。

**参与段列表本身必须保留**（用户提过能不能简化成"全部 or 某一个"）：实测两条**常量键 INSERT**
（真正触发直接分发）打不同段时 `segs = {1,2}`，而 **seg0 的 WAL 里零条 PREPARE 记录**——
它根本没被分发到，④ 也变不出它的 announce。直接分发让参与集是**任意子集**。
（注意区分：`UPDATE … WHERE key=const` **不**触发直接分发，全段都 prepare。）

### COVER1 — 覆盖判断提前到等待之前 —— **已做**（`0a7f85c379f`，2026-09-29）

`driver_collect_ready()` 里覆盖判断原本关在 `txn_complete()` 通过之后，而且要 `rel_names`
（只有段报告过才有）。于是**建槽边界那一类**（段侧 PREPARE 早于段槽起点，如日志里的 `gxid 19`）
——本来就被切点覆盖、数据已在 COPY 里、那个段永远不会报告——必须先熬满 10 秒宽限期才轮到被丢。
它还是 worker 启动后看到的第一笔，所以每次启动都白等 10 秒，后面全堵着。

改法：marker 行存的是**所有表里最早的那个切点**（`sync_cut_any()` 取 `relname == NULL` 的
catch-all），而覆盖按切点顺序**单调**——最早的切点覆盖 ⇒ 每张表的切点都覆盖 ⇒
**不需要知道它碰了哪些表**，可以在等任何人之前就整笔丢掉。纯粹加一个提前出口，不改已有语义。

**这是删掉宽限期的前提**；另一个前提 ①（`copy_data=false` 也跑屏障取切点）**还没做**——
那种订阅 `nsync_cuts == 0`，拿不到覆盖证据。

### NOCOPY1 — `copy_data = false` 也要跑屏障和切点 —— **已修**（`d21c0fd7f05`，2026-09-29）

`LrSyncMarkNoCopy()` 原来把 marker 直接写成 `'r'`、`sync_snapshot = NULL`，等于
**S1 建槽 / S2 屏障 / S3 切点全跳过**，只有 S4 该跳。后果两条：

1. **静默丢半笔事务**。逻辑槽在每个源节点各自有起点，建槽那一刻在飞的分布式事务
   会「有些段的部分在流里、有些不在」，把到货的那半应用上去 = 写了半笔事务。
   屏障就是等这些事务全部结束的那道闸，切点就是事后把它们整笔丢掉的依据 ——
   两者跟「拷不拷行」毫无关系。
2. **没预建槽的话反而会全量拷**。marker 读成 `'r'` 而一个槽都没有，正是「槽丢了」的样子，
   于是走 `sync_handle_slot_loss` → 默认 reinit → **做一次带 COPY 的完整同步**，
   把 `copy_data = false` 的意思整个反过来。

改法：marker 写 `'n'`（新状态，仅 relid = 0 用），worker 照常跑 S1–S3、只跳 S4。
**一个切点分给全部表**就够：这里没有 COPY 压着源端 xmin，「每表一个切点」的理由
（一个 lane 在某张表的拷贝里、不在另一张的）不存在。
用例 `initial_sync` 第 3 节改写：目标端种子行写 `'target'`，源端写 `'seed'`，
**拷了就看得出来**（实测 `still_the_targets=300 / copied_from_the_source=0`），
并断言 marker 和表行**都拿到了切点**（`with_a_cut_point = 2`）。

**代价说清楚**：被切点覆盖的那批 straddler 会被丢掉，也就是算进「你声明目标端已经有了」
的那部分。所以 `copy_data = false` 该在**源端安静**的时候声明。已写进 README。

### GRACE1 — 删掉宽限期和 `seg_moved_past` —— **已做**（2026-09-29）

EMPTY1 之后「被分发到的段 = 会 announce 的段」，等待本可以无条件；
挡着的是唯一一类等不到的情况：**某段的那笔在该段槽起点之前**，reader 永远看不到。
COVER1 + NOCOPY1 之后这一类**被判定而不再被猜**：它必然是建槽时在飞的事务，
S2 屏障等它结束、切点因此覆盖它（数据在初始 COPY 里），
`driver_collect_ready()` 在等任何人之前就整笔丢掉。**每条订阅路径都有切点了。**

于是 `txn_complete()` 只剩「参与段是否都报到」。

**但 `seg_moved_past()` 不能直接删 —— 它挡的是另一件事，实测出来的。**
把它删掉后 `publisher_ddl` **永久挂死**在 gxid 368317 上；只恢复它（宽限期仍删着）18.6s 通过。
根因是**协调器事件的重放**：协调器流在重连时从「上一次写下的位置」重读，
而那个位置只在**批提交**时才写下来，中间的事件会再来一遍。
第二次来的时候没有任何人还能认出它——段那边的重放起点已经越过它
（实测 gxid 368317 段 0 部分在 `9/5FC6C0D0`，batch 1 后段 0 重放起点 `9/5FC6C110`），
`gp_subscription_applied` 里的去重记录也随 `seg_flush` 前进被裁掉了。
`driver_handle_event()` 里原有的 `if (t->in_batch) return;` 只挡**还在当前批里**的重放，
批一提交就失效。

**正面解法**：按 `d->coord_committed` 判重放 —— 事件按位置顺序到达，
**位置 ≤ 已处理到的协调器位置 = 见过**，在注册之前就丢掉。
比 `seg_moved_past` 更准（它是「同段后来有事务报到」的反证），
也更省（那是每笔一次 O(n) 扫整个 gxid hash）。`publisher_ddl` **18.3s → 13.7s**。

**保留了一条诊断**：段安静超过 `10 × event_wait_ms` 就 WARNING 一次，
**只报不让步**——等待不会结束，也不会跳过任何段。`LR_REPORT_OPEN` / `parts[].open` /
`seg_open` 因此留着，但用途从「要不要放弃」降级为「这段是正在传还是真哑了」，
让这条 WARNING 只在真安静时才响（大事务传多久都不算安静）。
等不到不再意味着丢数据，而是意味着出了别的问题，警告的措辞也照这个改了。

### COVER1 是死代码 —— **已修**（`5f55a25dde4`，2026-09-29）

`sync_cut_any()` 找 `relname == NULL` 的兜底切点，而 `sync_load_cuts()` **只为 `relid <> 0` 建条目**，
marker 行单独读、只解析 local 那一半塞进 `cfg->sync_local`，**兜底条目从来没被造出来**。
于是 `cut_covers(sync_cut_any(d), ...)` 恒假 —— COVER1 那段一次都没执行过。
写 `sync_cut_any()` 时假定了一个不存在的条目，没核对；当时全套绿是因为**宽限期还在兜底**
（同一批事务几秒后被"放弃"掉，结果碰巧一样）。

删掉宽限期后立刻暴露：`initial_sync` 并发负载那节 **815 秒后失败**，
`still waiting for segment 0 to report gxid 388539 (0 of 3 participants reported)` ——
而该订阅的 `DS_t xmax = 388542`，388539 **本来就该被覆盖判掉**。

改法：marker 的切点也进数组，`relname` 留 NULL。`sync_cut_for()` 里本来就写着
`if (c->relname == NULL) any = c;  /* the catch-all, if there is one */` —— 整套代码都在等它。

**这笔事务是什么**：协调器的槽比段的槽**先**到达一致点（`gp_create_logical_replication_slot`
的协调器臂和各段臂各自独立跑 `DecodingContextFindStartpoint`）。在这中间提交的分布式事务，
协调器流里有它的提交记录（产生事件），而各段都在自己的建槽等待里把它跳过了 → **0/3 报到**。
它的数据在初始 COPY 里，所以正确处理就是整笔丢掉 —— 正是 COVER1 该干的事。

### BARRIER1 — 屏障 S2 整个删掉 —— **已做**（`81c938cf1b1`，2026-09-29，用户决定）

**先发现判据是空的**：`sync_barrier()` 等 `xmin >= xmax_pre`，而 `xmax = latestCompletedGxid + 1`；
`CreateDistributedSnapshot()`（`procarray.c:2632`）里 **`gxid >= xmax` 的在飞事务被整个跳过** ——
既不进 `in_progress` 也不压低 `xmin`。安静源端上最新那笔永远满足。
**实测**：在飞事务 gxid 387877，快照报 `xmin=387877 xmax=387877 in_progress={}` → 一来就过。

**再发现它本来就是多余的**：真正等"建槽时在飞的事务"的是**建槽自己** ——
`pg_create_logical_replication_slot(..., find_startpoint = true)` →
`DecodingContextFindStartpoint()`（`logical.c:643`）读 WAL 直到 `SNAPBUILD_CONSISTENT`，
判据是 `SnapBuildFindSnapshot()` 的 `oldestRunningXid >= next_phase_at`（要走满
BUILDING → FULL → CONSISTENT 两轮）。**prepared 事务有 dummy PGPROC、会进 `RunningXacts`**，
而"某段上已 prepare 未提交"正是唯一能横跨槽边界的形态。
**实测：15 秒事务在跑时建槽阻塞 13 秒**（无超时，只能靠信号打断）。

所以换成 `gp_get_next_gxid()` 阈值（会工作）的结果是**每次都等到零** —— 用户据此决定整个删掉。
一并删掉 `sync_snapshot_field()`、`sync_wait()`（没有别的调用者）、`LR_SYNC_BARRIER_TIMEOUT_MS`。
步骤重编号：**S1 建槽 → S2 切点 → S3 COPY → S4 追平**。

**协调器和各段建槽之间没有任何协调**，各自到达各自的一致点（槽本来就非事务）。
把两边接起来的是**顺序**：S1 整条语句返回 → S2 取切点。

**「QD 记账缺口」已经不存在了**（本节原先写它"靠时序关闭、不是不变式"——那是在
建槽顺序改成先段后协调器之前写的，已过时）。缺口原本描述的是:QD 写完提交记录、
还没跑 `ProcArrayEndGxact` 的那几条指令里取了切点,于是一笔段侧已全部结束的事务
仍被算作在飞、不被切点覆盖、**COPY 里没有它**。

现在它是良性的,而且由不变式保证:

1. 在切点时仍算未结束 ⟹ 它在**协调器的 FULL_SNAPSHOT 之后**才开始
   （那一刻还在跑的事务必须全部结束,协调器的槽才能到达一致点,而这笔在 C_QD 之后还活着）
2. **先段后协调器**（`7118f68873f`）⟹ 协调器的 FULL_SNAPSHOT 晚于所有段的一致点 C_j,
   更晚于所有段的 FULL_SNAPSHOT
3. ⟹ **它在每个段的 FULL_SNAPSHOT 之后才开始 ⟹ 每个段都完整捕捉了它**

所以"切点不覆盖"恰恰意味着"流里一定有",两条路径互补、不重不漏:
覆盖 ⇒ COPY 有它、丢掉流里那份;不覆盖 ⇒ COPY 没有、从流里应用。
这个"一定"在**协调器先建槽**的旧顺序下不成立——那时会出现"有事件、段却给不出"。
所以缺口不是被时序盖住的,是被建槽顺序消掉的。

（曾经考虑用 nextGxid 屏障显式关掉它,代价是连**只读长事务**一起等
——实测只读事务在飞时 `next_gxid=387882` vs `xmin=xmax=387881`——判定不值;
建槽顺序改对之后这条路也不需要了。）

### PGBUG1 — 上游残留的 `base_snapshot` 守卫会崩段 —— **已修**（`fd70493d446`，2026-09-29）

**现象**：段的 walsender 在同一 LSN 反复
`FATAL: Unexpected internal error (assert.c:46)` +
`("txn->ninvalidations == 0", reorderbuffer.c:3051)`，每次带崩段 postmaster。
它把一次全套跑挂了 `crash_recovery`/`shutdown`，两个用例报的都是
`Error on receive from seg0: server closed the connection unexpectedly` ——
**看到这种错先去段日志查崩溃，别查用例**。
**3 分钟产生 313 个 core、134 GB，把磁盘写满**（本会话第三次）。

**它是什么**（从 PG 自己的历史里挖出来的）:
- `b89e151054a`（2014 引入逻辑解码）里失效消息是在**该事务自己的历史快照下**执行的:
  `SetupHistoricSnapshot(txn->base_snapshot, txn->tuplecid_hash)`。
  所以 `txn->base_snapshot != NULL` 是**字面意义的前置条件**,`else Assert(ninvalidations == 0)`
  是"不该出现有消息却执行不了"的声明。
- `c6ff84b06a6`（2016, Andres Freund）引入 `ReorderBufferImmediateInvalidation()`,
  改成**故意在没有有效事务的状态下执行**——注释:*"entries will just be marked as invalid
  without accessing the catalog... we don't need to setup the full state necessary for
  catalog access"*。**快照的需求没了,守卫和断言留下了。**
- `a271a1b50e9`（PG14 prepare 时解码）把这段原样复制进新写的 `ReorderBufferInvalidate()`。

**为什么 commit 路径不炸而 prepare 路径炸**:`SnapBuildCommitTxn()`（`snapbuild.c:1199`）
专门给目录变更事务补一个 base snapshot,注释就是 *"we might need to execute invalidations,
add snapshot"*;`DecodeCommit()` 先调它、后判 skip。**prepare 不是 commit,没人调它。**
（那行只在"改了目录但没有任何可解码变更"的事务上触发——有数据变更的早被
`SnapBuildProcessChange` 建过了。）

**为什么 Cloudberry 上必撞**:vanilla PG 要有人真的用 `PREPARE TRANSACTION` 做 DDL,少见;
**CBDB 上普通 DDL 本身就是两阶段**,每个段都写带失效消息的 PREPARE。
只要有一条 `two_phase` 槽长期挂着(比如手测订阅),而测试套不停建库删库 —— 必撞。

**改法**:`ReorderBufferForget()` 和 `ReorderBufferInvalidate()` 里去掉
`txn->base_snapshot != NULL &&` 和配套的 `else Assert(...)`,直接
`if (txn->ninvalidations > 0) ReorderBufferImmediateInvalidation(...)`。
`ReorderBufferReplay()` 和流式路径(`reorderbuffer.c:2681`、`:4062`)有同样的假设,
但它们是提前 return 而不是断言,改它们是改行为不是删残留,**没动**。

**验证**(拿不到隔离的复现,所以用原始条件):`demo_sub` 活着的同时跑全套 ——
修复前 3 分钟 313 个 core、`crash_recovery`/`shutdown` 被带崩;
**修复后 0 core、三个段 0 断言、0 signal 6、30 绿 + 2 绿,订阅保持 streaming**。
`--disable-cassert` 构建上本来就不崩(空操作),但失效消息会被丢掉;现在会真的执行 ——
那正是这两个函数注释里一直声称要做的事。

**上游未修**:master 至今保留同样的写法(2026-09-29 核对)。值得报 pgsql-hackers。
先例:2024 年有人从另一个根因(top/sub 关系丢失)撞到同一条断言,社区**没动断言**,
而是去修 `SnapBuildCommitTxn()` 的不变式 —— 所以报告要讲清楚"守卫在 c6ff84b06a6 之后
已无功能作用",而不是"断言太严"。

### DUTY1 — 传输慢的真因：reader 只在 drain 里读 —— **已修**（`2c3c4453cd1`，2026-09-30）

**先前的结论是错的。** 09-28 在 release 上量 10M 单事务，得出"传输 51s 占 80%、
apply 6s 占 10%，但没有任何进程接近饱和（walsender 27–35%、reader 2–3%）"，
当时把它记成"瓶颈未定位"。那两个 CPU 数字本身就取错了进程。

**真因是结构性的**：reader 是个 QE，只有在执行 `gp_lr_drain()` 期间才会调 `recv`。
而驱动在**拿到 commit 事件之后**，只带预算派发过**一次** drain（`driver_handle_event`
里的 `Min(event_wait_ms, 500)`）；此后这笔事务靠空转轮询去追，而空转 drain 传的是
`wait_ms = 0` —— 意思是"把 socket 里现成的拿走就回来"。于是一笔大事务到达期间，
每隔 `idle_drain_ms`（默认 2000ms）才有人清一次 socket，其余时间 TCP 窗口关着，
源端 walsender 全卡在 `WalSenderWriteData`。

**实测**（:7100 三段 → :7300 两段，cassert，2,000,000 行单事务）：

| | reader 处于 `active` 的采样占比 | walsender CPU | reader CPU | 追平 |
|---|---|---|---|---|
| 修复前 | **5%**（100 / 1920） | 47 / 46 / 44 % | 24 / 13 / 23 / 7 % | 23.6 s |
| 修复后 | **83%**（1547 / 1856） | **77 / 74 / 50 %** | 25 / **72** / 24 / **66** % | **19.7 s** |

**"把 `idle_drain_ms` 调小"不是解法**：2000 → 100（GUC 下限）只把 23.6s 变成 16.6s
就到头了，因为零预算的 drain 每次只能搬走 socket 缓冲里那一点，节奏再密也补不上。
这一步的价值在于**证伪**——它说明问题不在间隔长短，而在"读取被绑在派发循环上"。

**改法**：在 `assembly_poll()` 里判断"手上是否有一笔已收到事件、参与段却没报齐的事务"
（`driver_wait_target()`，与 `txn_complete()` 同一个判据，去掉它的告警副作用）。有，
就**不在协调器的 socket 上睡**（`timeout = 0`），改为带 `event_wait_ms` 的预算、以该
gxid 为 target 去 drain —— 要等的东西本来就在 reader 的 socket 上。`LrReaderReceive()`
本来就在 target 就绪时立刻返回，所以预算放大不会让任何东西变慢；初始同步在飞时按
`sync_poll_ms` 收紧，免得饿着它。

**与上游 PG 的差别**：PG 的 apply worker 自己持有 walreceiver 的 socket，常驻在
`LogicalRepApplyLoop()` 的 `WaitLatchOrSocket` 里连续收，天然没有这个问题。我们的
reader 不占有主循环，只在被派发时才活 —— 这是 MPP 形态换来的代价，所以"等待必须
发生在 drain 内部"是这个架构的一条不变式，不只是一次调优。

**教训**：三个进程都不饱和、机器七成空闲，不是"找不到瓶颈"，是**没人在跑**本身就是
答案 —— 该去问谁在等谁，而不是继续 perf。判据是 `pg_stat_activity.state`（`active`
vs `idle`），不是 CPU 百分比。

### DUTY2 — 一个 reader 持有多条流时，除第一条外全被饿死 —— **已修**（`80500898ca0`，2026-09-30）

DUTY1 修完之后在 release 上量 10M 单事务，传输仍然没人饱和（walsender 40/40/**30**%）。
那个 30% 就是答案：`do_drain()` 顺序遍历本 reader 的所有流，而等待预算是**整个 DRAIN
共享**的 —— 第一条流把 `wait_ms` 用光，后面的流拿到 `budget = 0`，只捞 socket 里现成的
就返回。3 段源 → 2 段目标时正好有一条流长期处在这个待遇下，它的源端 walsender 永远
落后、永远最后收尾。

因为 DRAIN 是一条分发语句、要等所有 QE 返回，**全局速度被最慢的那个 reader 拉平** ——
所以三个 walsender 会一起停在同一个偏低的水平上。

**改法**：按轮读。round 0 不等待，把每条流已缓冲的全部取走；之后每一轮把剩余预算按
还在等的流数均分，各自在自己的 socket 上等一份。报告挪到轮次之后，每条流每个 DRAIN
一份 —— 这就是把 `drain_one()` 拆成 release / receive / report 的原因。

**实测**（release，:7500 三段 → :7700 两段，10M 行单事务）：

| | 追平 | 三个 walsender 的 CPU |
|---|---|---|
| 修复前 | 53.1 s | 40 / 40 / **30** % |
| 修复后 | **35.6 s** | **36.8 / 37.2 / 36.8 %** |

**顺带纠正一个结论**：DUTY2 修复前，`event_wait_ms` 越小越快（1000→53.1s，100→43.7s），
当时以为"drain 预算越短越好"。**那是假象** —— 短预算只是让被饿的那条流更早轮到。修完
之后 100ms 和 1000ms 在噪声范围内（38.1 vs 35.6–38.0，各跑三次）。默认值仍按用户要求
定在 100ms，理由改成"reader 持有很多条流时退化得更平缓"，不是"更快"。
`LR_EVENT_QUIET` 同时从 `10 × event_wait_ms` 改成绝对的 10 秒。

**剩下的 35.6s 花在哪**（10M 行单事务，release，3→2）：

```
insert      12.1 s   源端自己的事
transfer  ~17   s    到 t≈29s 三个 walsender 全部 WalSenderWaitForWAL
apply     ~19   s    t≈29s → 48.7s，期间零传输，行在最后整体出现
```

**传输与 apply 完全不重叠**，这是目前最大的一块：一笔事务要等最后一个字节到齐、
参与段全部报到才开始应用。两条可走的路见工作项 DUTY3 / OVERLAP1。

### CATCHUP1 — 增表 catch-up 提交会扔掉手上那批普通事务 —— **已修**（2026-09-30）

**症状**：`refresh_publication` 在 **release 构建**上稳定失败（`rp_a` 2693 / 2720），
cassert 的集群上一直是绿的。少的正好是**三笔完整的 churn 事务**：每笔 −10 条插入、
+1 条本该删掉的行 = −9，×3 = −27;rp_b 每笔丢 1 插入 + 1 删除正好抵消,
所以行数对得上、digest 对不上。

**根因**：`unit_committed()` 的 catch-up 分支调 `MemoryContextReset(d->batchcxt)`，
但不像正常路径那样先清空并应用 `d->batch_gxids` / `d->batch_locals`。而：

- `driver_collect_ready()` 把事务放进 `d->batch_gxids` 时，链表单元就在 `batchcxt` 里，
  同时给事务打上 `in_batch = true`；`driver_drain()` 把 `d->locals` 搬进
  `d->batch_locals` 也一样；
- 增表拷贝结束切到 catch-up 时，**驱动仍在读源**（`assembly_poll` 照跑），所以
  catch-up 期间到达的提交事件照样被收进批次；
- 那一批从未被应用就随 reset 一起没了，`d->batch_gxids` 变成悬垂指针（日志里
  "batch N committed: **0** gxid txns" 就是它读成空的样子），而事务在注册表里的
  `in_batch` 仍是 true —— `driver_collect_ready()` 此后**永远跳过它们**；
- `coord_committed` 随后继续前进越过它们，再无翻案余地。

**取证过程**（值得记下来，这类"无声丢行"只能这么查）：给驱动里每一条能让事务消失的
路径都加上 `DEBUG1` 日志——事件到达、五种丢弃、批次纳入、catch-up 丢弃——然后对着
gxid 序列看。结论是 `EVENT gxid 42922 / 42925 / 42926` 都到达且**没有任何一条丢弃日志**，
却从未进过批次，紧接着就是那个 0 笔事务的空批次。这些日志已按 `DEBUG1` 留在代码里。

**改法**（把设计里本就写着的不变式立住：catch-up 期间只发放被延迟的事务）：

1. `driver_collect_ready()` 在 `catchup_mode` 下直接返回，不收批次；
2. `driver_drain()` 在 `catchup_mode` 下不把 `d->locals` 搬进批次（它们分配在驱动的
   长生命期上下文里，留着下一批再走）；
3. `driver_sync_progress()` 手上还有未提交批次时不进入 catch-up（批次自己会按延迟收口，
   代价是一轮循环）；
4. catch-up 分支的 reset 改成"两个列表都空才 reset"+ `Assert` —— 万一将来又破了不变式，
   泄漏一个上下文也好过无声丢数据。

**验证**：修复前 6/6 复现；修复后 9/9 全过（6 次带 LOG 诊断、3 次诊断降到 DEBUG1 即
原始时序）；只撤掉这三处保护的 A/B 3 次里 2 次复现。

**为什么 cassert 看不见**：窗口只有十几毫秒，事件要正好落在里面。试过三种想让它确定化的
办法（紧循环 workload、`batch_max_latency_ms` 调大让批次常开、`batch_max_txns=1` 把
catch-up 拉成多次提交）**都不复现**，所以没有新增合成用例——回归靠 `refresh_publication`
本身，它在 release 上是必现的。**含义：只跑 cassert 的 CI 挡不住这一类问题。**

### WORKER1 — 一个订阅起了两个 worker —— **已修**（`7045efbcc43`，2026-09-30）

launcher 只凭共享槽决定要不要起 worker（`slot == NULL || slot->pid == 0`），而
worker 是**起来之后**才去 `lr_worker_attach()` 填 pid 的。落在这两步之间的一轮扫描
会为同一个订阅再起一个；`lr_worker_attach()` 见槽已被同 subid 占着，**直接覆盖
`slot->pid`**，没有拒绝。两个都跑起来：一个正常 drain，另一个永远在
`gp_lr_connect()` 上重试，每 2.3 秒一次，占着一个会话和每个目标段上的 reader gang，
直到集群关掉。

**窗口大到集群重启必然命中**：第一次去看就抓到两个订阅各两个 worker。

**改法**：槽上那个 pid 还活着（`BackendPidGetProc()`）就拒绝，并把两种拒绝理由分开 ——
抢输了不是槽不够，落败者退出也不是故障。

**发现经过**：是在重启目标集群加载新 `.so` 之后，顺手看 `pg_stat_activity` 撞见的
（两个 `mpp apply worker for subscription "s"`，etimes 都等于重启后的秒数）。

### SEC1 — copyhelper 的连接认证：令牌经 argv 泄漏（**未修**）

**现状不是"谁都能连"**——已经有令牌，而且真的校验：

| | |
|---|---|
| 令牌生成 | `sync_new_job()`（`mpp_tablesync.c`）：`pg_strong_random` 取 **128 bit**，拼成 `lr<subid>_<relid>_<32 hex>` |
| 校验 | 发送方第一帧必须是自己的 `--job`，接收方 `recv_greeting()`（`copyhelper.c`）比对，不符就断 |
| 监听地址 | 驱动传 `--bindaddr`（段自己的地址），不是 `INADDR_ANY`；`mpp_logicalrep.copy_bind_address` 可配 |
| 端口文件 | `$PGDATA/gp_lr/<job>.<seg>.port`，`open_private`（0600），目录 `mkdir(…, 0700)` |
| 已有测试 | `test/cluster/security.sh` 第 3、4 段 |

**缺口一（要紧的）：令牌走命令行。** 嵌在
`COPY … TO PROGRAM 'cbdb_lr_copyhelper send --job <token> …'` 里，于是出现在：

1. **源端和目标端每台段主机的 `ps`**——实测 `/proc/PID/cmdline` 是 `-r--r--r--`（0444），
   **任何本地用户可读**（本机未挂 `hidepid`）
2. 两侧的 `pg_stat_activity`（superuser / `pg_read_all_stats` / 同角色）
3. 开了 `log_statement` / `log_min_duration_statement` 的日志

对比：`/proc/PID/environ` 实测是 `-r--------`（0400），**只有同一个 OS 用户和 root**。

**缺口二**：`recv_greeting` 用 `strcmp`，不是常数时间。威胁很低（每猜一次要开一条 TCP、
只回一个比特），但换掉零成本。

**缺口三**：没有一次性、没有过期。`--clients N` 接受 N 条连接，令牌在整个 copy 窗口内可重放。

**拿到令牌能干什么**：连上目标段那个端口，把任意字节当 COPY 数据**写进正在拷的那张表**。
`gp_enable_segment_copy_checking` 只保证行落在对的段，不是安全控制。链路明文
（`--crc` 是 CRC32C，纠错不是认证），在途者能读也能注入。

#### 端口现在是怎么传的（令牌和它同路，所以两者耦合）

```
① 接收方 listen_port() 挑端口 → 写 $PGDATA/gp_lr/<job>.<seg>.port（0600，tmp+rename）
② 驱动向目标协调器轮询 copy_ports_ready():
     SELECT string_agg(a.address || ':' || p.port, ',' ORDER BY p.segid)
       FROM gp_lr_copy_ports($1) p            -- EXECUTE ON ALL SEGMENTS，各段读自己的文件
       JOIN gp_segment_configuration a ON …
      HAVING count(*) = $2                    -- 要么全齐要么不算
③ 结果进 s->cur_targets = "host0:port0,host1:port1,…"
④ copy_send() 拼进 "…send --job <token> --seg <SEGID> --targets <那串>"
   嵌入 COPY … TO PROGRAM … ON SEGMENT 分发到源端
⑤ 发送方挑 targets[S mod ntargets]，连上去，第一帧发 --job
```

端口不是秘密（扫一下就有），令牌是；两者共用第④跳，这就是问题所在。

#### 管道能不能用（实测过的两个事实）

`popen_with_stderr`（`src/backend/storage/file/execute_pipe.c`）两侧不对称：

| | `forwrite` | 子进程的管道 |
|---|---|---|
| 发送方 `COPY (q) TO PROGRAM` | true | **stdin 是管道**（backend 往里写 COPY 数据） |
| 接收方 `COPY t FROM PROGRAM` | false | **stdout 是管道；stdin 是继承的**，没接到 backend |

另有一条 stderr 管道，但方向是**子进程 → backend**。

**`COPY (query) TO PROGRAM` 不接受绑定参数**（实测：`there is no parameter $1`），
所以"把令牌塞进发送方 stdin"必须绕服务端暂存：先
`SELECT gp_lr_stash_token($1)`（扩展协议，参数不进 `pg_stat_activity.query`），
再让 COPY 的查询把它取出来放在流首。

#### 四个选项

1. **接收方自生成令牌，搭现有端口文件通道**（文件多一行、`gp_lr_copy_ports` 多一列）。
   **目标侧 argv 彻底干净**，而且令牌**天然一次性**（随作业生成）。
   要把今天 `--job` 身兼的两个角色拆开：文件名用的**作业标签**（不敏感）和**令牌**（敏感）。
   零协议变更，驱动在它本来就读端口的地方读到，不多一次往返。
2. **源端那一跳走环境变量**（`EXECUTE ON ALL SEGMENTS` 的函数在各 QE 里 `setenv`，用完 `unsetenv`）。
   **消掉 `pg_stat_activity` 和日志**，`ps` 收窄到同 UID。
   实测两条相关事实：setenv 加的变量**不出现在设置者自己的** `/proc/self/environ`
   （putenv 在堆上，那个文件读的是 exec 时那段栈），但**出现在被 exec 的子进程的** environ 里。
   三个坑：必须走**同一条连接**才是同一个 gang 的 QE；依赖两条语句间 **gang 复用**（不保证）；
   不 `unsetenv` 会留在池化的 QE 里。
3. **反向连接**：让发送方监听、接收方去连。
   冒充者连上去从"**往表里写任意行**"降为"**读走表的内容**"；要注入就得在真 sender 之前
   **抢占源端段主机上那个端口**——本机权限 + 赢一次竞态，比读一次 `ps` 高一个量级。
   代价：要在源端复刻①②那套端口发布（一个读文件的 SRF）。
4. **服务端暂存 + 管道**：同 ②，且**同 UID 的 `ps` 也没有**。
   比 ② 多的是：COPY 查询首行取令牌、源端会话态、helper 要剥首帧。
   好处是**不依赖 gang 复用**——令牌随数据流进去。

**一次误判记下来**：中途我推过"源端不需要装扩展，所以 ②③④ 都要加部署依赖"。
**错的**——`cbdb_lr_copyhelper` 和 `mpp_logicalrep` 是**同一个 module**
（`SUBDIRS = copyhelper`，一次 `make install` 同时装 helper、`.so`、`cdb_init.d` 脚本），
源端有 helper 就必然装了整个 module。那条推论的起点是"源端调用的五个 SQL 函数全是内核的"，
跳过头了。而且源端那个 setenv 函数**不需要 `shared_preload_libraries`**，按需加载即可。

**建议**：**①+②** 是低代价拿掉大半暴露面（①零依赖且带来一次性，②消掉两条路径）。
若在意 gang 复用的脆弱性，用 ④ 换 ②。③ 是改威胁模型，单独决策。

**估**：① 半天；② 一两小时；③ 中等；④ 一天。**与 P1–P4 无依赖**。

### P1 — 增表路径的节奏 —— **已修复**（`4a54798269e`，2026-09-25）

**曾经的问题**：见 2.3。每表两态，每格要等一次 poll；空闲流上那次 poll 等满
`gp_lr_batch_max_latency_ms`（200 ms），于是每表 0.726 s，而初始同步同样的活只要 74 ms。

**改法**（落在 `assembly_poll`，就是那次等待本身所在的地方）：

```c
timeout = Min(timeout, timeout_ms);
if (d->sync != NULL)
    timeout = Min(timeout, d->sync_poll_ms);   /* 1 → 2 → 4 … → 100 ms */
```

`d->sync_poll_ms` 在作业启动时置 1，**每格有进展就重置回 1**，原地等同一件事才翻倍。
"有没有进展"由 `LrSyncAddStep(s, &advanced)` 新出参给出——这是和初始同步
`sync_copy_table` 逐阶段重置退避同构的做法，也是把 0.49 再拉到 0.33 的那一步。

**没走原计划的契约改动。** 原方案是给 `housekeeping` 加 `int *poll_ms`
让 assembly 告诉驱动。实现时发现多余：`assembly_poll` 本来就把驱动给的间隔
压到自己的 batch latency，assembly 完全可以单方面再压一道。改完实测两版等价
（0.49 vs 0.41），所以留了小的那版 —— `logicalrep_sink.h`、
`logicalrep_apply_driver.c`、单测 mock 一个字没动。

**仍然不能做的**：让 `driver_sync_progress` 返回 true。那是无 sleep 的热自旋。

### P2 — 每表一个切点 —— **已修复**（`ee24b616e81`，2026-09-25）

**曾经的问题**：一个 RR 事务从切点开到**所有表**拷完，源端 `xmin` 被压住，
vacuum 清不掉、WAL 回收不了。实测一笔长事务把**别的订阅的屏障挡住 200 秒**
（120s 超时是可重试错误 → worker 退出 → launcher 重启 → 再等，`disable_reason` 一直为空，
表现为无限期停滞而非硬失败）。

**改法**：每表自取切点、自持事务，只覆盖自己那一趟 copy。

**代价（自觉付的）**：丢掉"整套表的一致快照"——同步窗口内目标端是 A@T_A + B@T_B，
源端从未有过的状态。**表内**仍是源端历史的一致点；PG 也只给到这个程度
（它的 tablesync worker 同样一张表一张表拷）。

**过滤器随之改（主要工作量，落地方式与草案不同）**：

"已被拷过"从**事务的属性**变成**事务 × 表的属性**。判定留在**驱动侧**：

| 情形 | 处理 |
|---|---|
| 它碰的**每张表**都覆盖 | 照旧整笔丢（常见情形，开销不变） |
| **部分**覆盖 | 被覆盖的那些表的 relid **随这笔事务**下到 apply 计划的 `txns` 条目，
在 `source_emits_rel()` 处生效——emit 本来就走那儿 |

**按事务不按单元**：同一个单元里两笔事务对同一张表的结论可以相反，所以
`AssembledTxn` 加 `nskip/skip`，`txns` 条目扩成 `<seg>:<gxid>:<relid>.<relid>…`，
`LrApplyItem` 带着它，`state->cur_item` 在 emit 时就在手上。

**没走草案说的"喂 `skip_relids`"**：那是**单元级**的扁平集合，表达不了同单元内的分歧。

**local 事务同理**，按各表自己的本地快照判，只有一个例外：**reader 仍按最早切点整笔丢**。
reader 独自决定、手上没有关系名，所以只能用最早那个；而**覆盖关于切点次序是单调的**
（被早的覆盖 ⇒ 被所有晚的覆盖），所以那恰好就是"全覆盖"那一类，剩下的照常上报给驱动。
这里复用了 P0 时就已经把 local 事务 xid 送到驱动的那条通路。

**存储**：`gp_subscription_rel` 每行存自己的切点，marker 行存**最早**的那个
（reader 的本地半边只能用它）。

**用例** `isolation2/per_table_cut`：合成出整笔过滤器**不可能**产生的局面——
一笔事务写两张表，切点一个取在它之前、一个取在它之后，结果行落进前者、不落进后者。

**踩到的坑**：① 第一版 `sync_filter` 只往 marker 行写合成切点，改布局后失效——
那是**测试写死了旧存储布局**，不是产品问题。② 第三次栽在 isolation2 的 `::` 上
（`'pc_a'::regclass` 静默 FAILED），要写 `cast('pc_a' as regclass)`；
而且当时的断言仍为真（真实的两个切点本来就不同），差点把错误结论当成产品 bug。

**未做**：增表路径（`add_start_body`）仍是**每个作业一个切点**，一次 REFRESH 多张表时
那笔 RR 事务照样横跨整个作业。`deferred_txns` + `gxid_covered_by_add_sync` 是整作业语义，
改它是另一块。

### P3 — 表间并行 —— **已做**（`0d2c01163ee`，2026-09-25）

**形态**：`LrSync` **本身就是一条 lane**（自带 `src`/`tgt`/`step`/`cur_*`），所以不做结构手术，
直接开 K 个实例，各自一条源端会话 + 一条目标端会话 + 自己的 `tgt2`，从**共享的发放游标**
取表——慢表不会卡住本可以干活的 lane。**这条路是 P2 之后才成立的**：切点搬到
`LrSyncRel` 上之前，一笔事务、一个快照属于整套表。

**实测**（3 段单机，50 张 200 行的表）：

| K | 拷贝段 | 总 |
|---|---|---|
| 1 | 4.56 s | 7.60 s |
| **2** | **2.31 s（1.97×）** | 5.52 s |
| 4 | 2.77 s | 6.26 s |
| 8 | 2.71 s | 6.56 s |

8 张 30 万行的表同样在 K=2 饱和（3.99 / 3.07 / 3.12）。**两路并发就把这台机器吃满了**，
再加只是加开销，所以 **默认 2**——PG 的 `max_sync_workers_per_subscription` 也是 2。
主机更多、有余力的集群可以调高，GUC `mpp_logicalrep.max_concurrent_copies`（1–16），
并按端口范围钳位（每条 lane 的接收方在每个目标段上各占一个端口）。

**一个坑**：lane 从没连过源端——老代码是在屏障那一步顺带连上的，
新 lane 不走那条路，于是 `sync_cut_point` 撞上 `connection pointer is NULL`。
`sync_lane_open` 里补 `sync_connect_source(lane)`（也是它 `nsrcsegs` 的来源）。

### P5 — 前段批量化 —— **已做**（`7504a583ee8`，2026-09-25）

**问题**：拷贝开始前要逐表问目录——目标端的列、源端能不能解码、两侧摆放是否一致、
两侧的键。**每表 7 次往返**，还在两条连接之间来回切；MPP 里一条目录查询要规划、
可能建 gang，不便宜。50 张表在这上面花 **2.7 s**，串行、在屏障之前，**加多少 lane 都够不着**。

**改法**：这些都是"对每个名字问同一个问题"，所以一次问完——名字作为一个 `text[]` 下去，
`unnest(...) WITH ORDINALITY` 保住列表顺序，`LEFT JOIN` 让不存在的名字也留一行
（"目标表不存在"那个报错才还在）。状态行同理，一条 `INSERT … SELECT unnest()` 代替 50 条。
**总共 7 次往返，与表数无关。**

**实测**：前段 **2.69 s → 0.31 s（8.8×）**；整轮同步单 lane **7.91 → 4.77 s**，
双 lane **5.52 → 3.8 s**。前段的成本原本随表数线性增长，现在不再增长——
几千张表的 schema 才是这条真正要救的。

**没加新用例**：问什么、判什么都没变，所以是那些本来就在考这些答案的用例在作证——
`unsupported_tables`（存取方法）、`distribution_keys`（键契约）、`direct_copy`（摆放比对）。

**踩到的坑**：第一次改 `sync_fill_source_info` 时，Python 的锚点匹配到了**前向声明**而不是定义，
一口气删掉 755 行。回滚重做，改成用 `'\nstatic void\nfoo(...)\n{'` 这种带上下文的锚点，
并在替换后校验被删区段的长度。

### P6 — 每表往返次数（切点三合一已做 `fe9f719fe41`；**其余用户决定不做**，2026-09-25）

> **不做的是**：①驱动直接分配端口（省掉 helper 写端口文件 + 驱动那 4 次全段分发查询，
> 是每表 62 ms 里最大的一块，与 SEC1 的"反向连接"耦合在同一套端口发布机制上）；
> ②`sync_set_rel_state` 的 UPDATE 并进别的目标端语句。
> 每表往返次数就停在这里。下面的测量和分解留着，将来要接着做时那张往返表就是起点。

P5 之后重新量的构成（3 段单机，K=2 默认）：

```
T ≈ 0.76 s  +  N × 62 ms  +  行数 × 0.76 µs
    └固定┘     └每表延迟┘      └数据吞吐┘
```

对得上实测：50 表 × 200 行 → 0.76 + 3.10 + 0.01 = 3.87 s。

| 场景 | 固定 | 每表延迟 | 数据 | 总 |
|---|---|---|---|---|
| 50 表 × 200 行 | 0.76 s（20%） | **3.10 s（80%）** | 0.01 s | 3.87 s（实测） |
| 10 表 × 20 万行 | 0.76 s（28%） | 0.62 s（22%） | **1.52 s（50%）** | 2.75 s（实测） |
| 1000 表 × 200 行 | 0.76 s（1%） | **62 s（99%）** | 0.2 s | ~63 s（外推） |

**表多而小卡在每表延迟，表少而大卡在数据吞吐。** 每表 62 ms 与数据量无关
（20k 行以内和 200 行一样快），是 **~8 次往返 × 8 ms**：

| 步 | 连接 | 往返 | 备注 |
|---|---|---|---|
| ~~`sync_cut_point` BEGIN + 两个快照~~ | src | ~~3~~ → **1** | **已合并**（`fe9f719fe41`） |
| `sync_set_rel_state` | tgt2 | 1 | |
| `copy_begin` 的接收 INSERT | tgt | 1 | 异步发出 |
| **`copy_ports_ready` 轮询** | tgt2 | **≥2，通常 4** | **全段分发**，占大头 |
| `copy_send` | src | 1 | 异步发出 |
| COMMIT | src | 1 | 每表一笔事务，P2 要求 |

**等端口那圈是大头**：helper 要 fork/exec/bind/写文件几十毫秒才就绪，而每次轮询本身就是
8 ms 的全段分发——1→2→4 ms 的退避比查询还短，等于"每 9 ms 问一次，问到第 4 次才有"。

**已做**：切点三条合成一条 `PQsendQuery`（`sync_exec_multi`）。
实测 50 表 3.79 → **3.45 s**，每表 74 → 69 ms。省得比两次往返少，
因为省掉的是两条**协调器本地**的廉价语句，那条分发的快照查询还在。
线上验过两条语义：多语句里的 `BEGIN` 事务留着（后续语句 `SHOW transaction_isolation`
= `repeatable read`），且执行顺序保持（分发语句在协调器语句之前）。

**未做，按价值排**：

1. **驱动直接分配端口**（最值钱）。今天是 helper 挑端口 → 写文件 → 驱动分发查询去读。
   驱动手上已有端口范围、段数、lane 数，可以自己分配，一次性省掉 helper 写文件和
   **那 4 次全段分发查询**。代价是端口冲突自理（绑不上就让 helper 报错退出，驱动换一个）。
   **与 SEC1 的"反向连接"是同一片地，一起设计更省。**
2. `sync_set_rel_state` 那条 UPDATE 并进别的目标端语句，省 1 次。

乐观估计 1+2 能把每表压到 2–3 次往返（~20 ms），50 表 → 约 2.2 s，1000 表 → 约 21 s。

**K 调大没用**（P5 之后重测）：50×200 行 K=1/2/4/8 = 4.91 / 3.71 / 3.70 / 4.46 s；
10×20 万行 K=1/2/4 = 3.84 / 2.60 / 2.62 s。**两种形状都在 K=2 饱和**，K=8 倒退。
原因是这台机器本身（3 段挤一台主机）已被两路吃满；而每表那 62 ms 是**串行延迟不是带宽**，
每条 lane 各等各的，加 lane 摊不掉。多主机集群才可能重新有收益。

### P4 — `done` 改成在 socket 上等 —— **已做**（`1fc279da744`，2026-09-25）

**问题**：`copy_poll_done` 是 `PQconsumeInput` + `PQisBusy`——问一次不要钱，但不问就不知道，
所以只能按退避去问，一趟提前结束的 copy 最多晚十分之一秒才被发现。

**改法**：在飞的 lane 两端各有一个"活干完就变可读"的 socket（源端的 COPY、目标端的 INSERT），
就等它们。**等在池上，不是等在 lane 上**——一条 lane 挂自己那两个 fd 会把别的 lane 全卡住，
而那正是分 lane 的意义。事件集按"哪些 lane 在飞"现建；**等端口的那一相仍由超时管**，
因为它等的是对端出现一个文件，没有 socket 可挂。

**实测**（与同一份去掉该改动的构建 A/B，各三轮）：

| 形状 | 无 | 有 |
|---|---|---|
| 10 表 × 20 万行 | 2.72 / 2.72 / 2.63（**2.69**） | 2.54 / 2.45 / 2.48（**2.49**）→ **1.08×** |
| 50 表 × 200 行 | 3.49 / 3.36（3.43） | 3.43 / 3.36（3.40）→ **无差别** |

小表在退避的头几格里就拷完了，还没来得及欠什么；**被晚发现的是真正花时间的那些 copy**。

---

## 四、已验证的关键事实（不必重验）

1. **复制表**：`gp_segment_id` 在其上不存在；`WHERE gp_execution_segment() = 0` 和派生表
   写法都**依赖规划器挑了哪个段**（`Gather Motion 1:1 (segments: 1)`），两个 optimizer
   结果不同且我拿到过互相矛盾的读数；只有 `gp_dist_random('<rel>')` 可靠
   （`Gather Motion 3:1 (segments: 3)`，两个 optimizer 计划相同，ORCA 回退）。
   压测 36 组合（2 集群 × 3 行数 × 2 optimizer × 3 轮）全过。已落地于 `e8738fd75ee`。
2. **接收端广播**：`INSERT INTO <复制表> SELECT * FROM gp_lr_copy_recv(...)` 的计划是
   `Broadcast Motion N:M`，每个目标段拿全量。
3. **`ON SEGMENT` 会在每个源段启动 PROGRAM**，过滤后 0 行的段也启动、也连、发空流；
   helper 按 `targets[S mod ntargets]` 挑目标，所以接收端 `--clients` 计数不变。
4. **既有的行数核对防不住"0 份"和"N 份"**（`cur_src_rows` 是源端 COPY 自报），
   所以复制表新增 `cur_want_rows`：拷贝前 `SELECT count(*)`，拷完比对。
5. **QE 本地快照的冻结点 = 第一条被分发的语句**。协调器专用语句
   （`gp_current_distributed_snapshot`，`proexeclocation='c'`）不冻结任何段。
   `COPY … TO STDOUT` 和 `COPY … TO PROGRAM … ON SEGMENT` 都共用这份冻结快照。
6. **只读的、会分发的 RR 事务照样持有 gxid**（实测 `inprog` 里有它），所以切点事务
   在整个拷贝期间都是一笔在飞的分布式事务。
7. **reader 从不自己读流**：读 socket 只发生在 `LrReaderReceive()` 里
   （`mpp_reader.c:9` 的注释就是这么写的），唯一调用点是 DRAIN UDF
   （`mpp_source.c:695-697`）。DRAIN 是 release → receive → feedback → report 四步，
   第 3 步无条件执行。所以"停止消费源端"只需要跳过第 2 步，且天然不会触发源端的
   `wal_sender_timeout`。
8. **崩溃重启不会因为 `applied` 去重丢掉增表数据**：`LrSyncAddAbort` 不重置状态行，
   重启后 `add_start_body` 先 `TRUNCATE` 再取**新切点**，之前半途应用过的事务虽被去重丢掉，
   但它的行已在新 COPY 里。

---

## 五、不做的事

| | 理由 |
|---|---|
| 照搬 PG 的独立 tablesync worker + 每表一个槽 | 槽在 MPP 里乘以 N+1，当前上限 10；gang 和连接的成本与 C 相同（成本在会话不在进程），唯一多出来的是"可以放心阻塞"；见 §1.2 |
| 共享一个导出快照做并行 | 各段本地快照带不过去，见 2.4 |
| 照抄 PG 对 copy 期间积压的处理 | PG 没有可抄的——它的 sync worker 自己从自己的槽把该表的增量 apply 掉，主 worker 直接丢弃（`worker.c:2410`），压根不存在需要反压的积压；它用"重复解码"换掉了"无界缓存"。我们的对策见 P0.5 |
| P6 剩余项：驱动直接分配端口 / 合并状态行 UPDATE | 用户 2026-09-25 决定不做；每表往返停在 ~6 次，见 P6 |
| 强制 `set optimizer` 保证计划形状 | 两个 optimizer 对 `gp_dist_random` 计划相同；真正的兜底是 §四.4 的行数期望值，与计划无关 |
