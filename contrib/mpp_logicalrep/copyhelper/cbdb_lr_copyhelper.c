/*-------------------------------------------------------------------------
 *
 * cbdb_lr_copyhelper.c
 *	  Segment-to-segment bulk transfer helper for MPP logical replication
 *	  initial synchronisation (design section 10.4).
 *
 * It only moves bytes; it knows nothing about tables, hashing or routing.
 * The target-side INSERT ... SELECT (gp_lr_copy_recv) does the redistribution
 * through the planner's Motion, so the helper is a dumb, framed TCP pipe.
 *
 *   recv --job J --seg S --clients N [--port-range LO-HI] [--bindaddr A]
 *       Listen on a TCP port, write "<port>\n" to $PGDATA/gp_lr/J.S.port
 *       (PGDATA taken from the environment), accept N client connections,
 *       read their framed streams and write the raw COPY bytes to stdout
 *       (a "COPY t FROM PROGRAM 'cbdb_lr_copyhelper recv ...'" reads them).
 *       On success write J.S.done with "rows? bytes\n" and exit 0.
 *
 *   send --job J --seg S --targets H0:P0,H1:P1,...
 *       Read raw COPY bytes from stdin (from "COPY (query) TO PROGRAM
 *       'cbdb_lr_copyhelper send ...'"), frame them and send to one target,
 *       chosen as targets[S mod ntargets].  Exit 0 when stdin reaches EOF and
 *       the end marker has been acknowledged by the peer closing.
 *
 * Frame: [uint32 net length][length bytes].  A zero-length frame ends the
 * stream.  Optional CRC32C per frame when --crc is given on both sides.
 *
 * Portions Copyright (c) 2026, Apache Cloudberry
 *
 * IDENTIFICATION
 *	  src/bin/cbdb_lr_copyhelper/cbdb_lr_copyhelper.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "common/logging.h"
#include "getopt_long.h"
#include "port/pg_crc32c.h"

#define LR_CHUNK	(256 * 1024)
#define LR_CONNECT_TIMEOUT_S	120
#define LR_ACCEPT_TIMEOUT_S		300

static bool use_crc = false;

static void
usage(void)
{
	printf("cbdb_lr_copyhelper - MPP logical replication segment-to-segment copy transport\n\n");
	printf("  cbdb_lr_copyhelper recv --job J --seg S --clients N [--port-range LO-HI] [--bindaddr A] [--crc]\n");
	printf("  cbdb_lr_copyhelper send --job J --seg S --targets H0:P0,H1:P1,... [--crc]\n");
}

/* write exactly len bytes or die */
static void
write_all(int fd, const char *buf, size_t len)
{
	while (len > 0)
	{
		ssize_t		n = write(fd, buf, len);

		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			pg_fatal("write failed: %m");
		}
		buf += n;
		len -= (size_t) n;
	}
}

/* read up to len bytes; returns 0 on EOF, len on full read, dies on error */
static size_t
read_some(int fd, char *buf, size_t len)
{
	ssize_t		n;

	do
		n = read(fd, buf, len);
	while (n < 0 && errno == EINTR);
	if (n < 0)
		pg_fatal("read failed: %m");
	return (size_t) n;
}

static bool
read_full(int fd, char *buf, size_t len)
{
	size_t		got = 0;

	while (got < len)
	{
		size_t		n = read_some(fd, buf + got, len - got);

		if (n == 0)
			return false;		/* EOF */
		got += n;
	}
	return true;
}

/* ------------------------------------------------------------------------
 * send
 * ------------------------------------------------------------------------
 */

static int
connect_target(const char *hostport)
{
	char	   *copy = pg_strdup(hostport);
	char	   *colon = strrchr(copy, ':');
	char	   *host;
	char	   *port;
	struct addrinfo hint;
	struct addrinfo *res;
	struct addrinfo *ai;
	int			fd = -1;
	int			rc;
	time_t		deadline = time(NULL) + LR_CONNECT_TIMEOUT_S;

	if (colon == NULL)
		pg_fatal("bad target \"%s\", expected host:port", hostport);
	*colon = '\0';
	host = copy;
	port = colon + 1;

	memset(&hint, 0, sizeof(hint));
	hint.ai_family = AF_UNSPEC;
	hint.ai_socktype = SOCK_STREAM;

	for (;;)
	{
		rc = getaddrinfo(host, port, &hint, &res);
		if (rc != 0)
			pg_fatal("could not resolve %s:%s: %s", host, port, gai_strerror(rc));
		for (ai = res; ai != NULL; ai = ai->ai_next)
		{
			fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
			if (fd < 0)
				continue;
			if (connect(fd, ai->ai_addr, ai->ai_addrlen) == 0)
				break;
			close(fd);
			fd = -1;
		}
		freeaddrinfo(res);
		if (fd >= 0)
			break;
		if (time(NULL) >= deadline)
			pg_fatal("could not connect to %s:%s within %d s", host, port, LR_CONNECT_TIMEOUT_S);
		pg_usleep(100 * 1000);
	}
	pg_free(copy);
	{
		int			one = 1;

		setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
	}
	return fd;
}

static void
send_frame(int fd, const char *buf, uint32 len)
{
	uint32		nlen = pg_hton32(len);

	write_all(fd, (char *) &nlen, 4);
	if (len > 0)
	{
		if (use_crc)
		{
			pg_crc32c	crc;
			uint32		ncrc;

			INIT_CRC32C(crc);
			COMP_CRC32C(crc, buf, len);
			FIN_CRC32C(crc);
			ncrc = pg_hton32((uint32) crc);
			write_all(fd, (char *) &ncrc, 4);
		}
		write_all(fd, buf, len);
	}
}

static int
do_send(const char *job, int seg, char *targets)
{
	char	   *list[4096];
	int			ntargets = 0;
	char	   *tok;
	char	   *save = NULL;
	int			fd;
	char	   *buf = pg_malloc(LR_CHUNK);
	uint64		total = 0;

	(void) job;
	for (tok = strtok_r(targets, ",", &save); tok && ntargets < 4096;
		 tok = strtok_r(NULL, ",", &save))
		list[ntargets++] = tok;
	if (ntargets == 0)
		pg_fatal("no --targets given");

	/* N_src == N_dst: seg i -> target i; otherwise round-robin by seg */
	fd = connect_target(list[seg % ntargets]);

	for (;;)
	{
		size_t		n = read_some(STDIN_FILENO, buf, LR_CHUNK);

		if (n == 0)
			break;
		send_frame(fd, buf, (uint32) n);
		total += n;
	}
	send_frame(fd, NULL, 0);		/* end marker */

	/* wait for the peer to close, so it has consumed everything */
	{
		char		tmp[64];

		(void) read_some(fd, tmp, sizeof(tmp));
	}
	close(fd);
	pg_free(buf);
	pg_log_info("send seg %d -> %s: %llu bytes", seg, list[seg % ntargets],
				(unsigned long long) total);
	return 0;
}

/* ------------------------------------------------------------------------
 * recv
 * ------------------------------------------------------------------------
 */

/*
 * Several helpers (one per segment of this host) scan the same range at the
 * same time.  With SO_REUSEADDR two of them can both bind() a port while
 * neither listens yet; the second listen() then fails with EADDRINUSE.  So a
 * failed listen() is not fatal: drop the socket and try the next port.
 */
static int
listen_port(const char *bindaddr, int lo, int hi, int *chosen)
{
	struct sockaddr_in sa;
	int			p;

	memset(&sa, 0, sizeof(sa));
	sa.sin_family = AF_INET;
	sa.sin_addr.s_addr = bindaddr ? inet_addr(bindaddr) : htonl(INADDR_ANY);
	for (p = lo; p <= hi; p++)
	{
		int			fd = socket(AF_INET, SOCK_STREAM, 0);
		int			one = 1;

		if (fd < 0)
			pg_fatal("socket failed: %m");
		setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
		sa.sin_port = htons((uint16) p);
		if (bind(fd, (struct sockaddr *) &sa, sizeof(sa)) == 0 &&
			listen(fd, 16) == 0)
		{
			*chosen = p;
			return fd;
		}
		if (errno != EADDRINUSE)
			pg_fatal("could not listen on port %d: %m", p);
		close(fd);
	}
	pg_fatal("no free port in range %d-%d", lo, hi);
	return -1;					/* unreachable */
}

static bool
recv_one_client(int cfd, uint64 *bytes)
{
	char		hdr[4];
	char	   *buf = pg_malloc(LR_CHUNK);
	bool		ok = true;

	for (;;)
	{
		uint32		len;

		if (!read_full(cfd, hdr, 4))
		{
			ok = false;			/* peer vanished before the end marker */
			break;
		}
		len = pg_ntoh32(*(uint32 *) hdr);
		if (len == 0)
			break;				/* clean end of stream */
		if (len > LR_CHUNK)
		{
			buf = pg_realloc(buf, len);
		}
		if (use_crc)
		{
			char		crcbuf[4];
			pg_crc32c	crc;

			if (!read_full(cfd, crcbuf, 4) || !read_full(cfd, buf, len))
			{
				ok = false;
				break;
			}
			INIT_CRC32C(crc);
			COMP_CRC32C(crc, buf, len);
			FIN_CRC32C(crc);
			if ((uint32) crc != pg_ntoh32(*(uint32 *) crcbuf))
				pg_fatal("CRC mismatch in a %u-byte frame", len);
		}
		else if (!read_full(cfd, buf, len))
		{
			ok = false;
			break;
		}
		write_all(STDOUT_FILENO, buf, len);
		*bytes += len;
	}
	pg_free(buf);
	return ok;
}

static int
do_recv(const char *job, int seg, int nclients, int lo, int hi, const char *bindaddr)
{
	int			lfd;
	int			port = 0;
	char	   *pgdata = getenv("PGDATA");
	char		dir[MAXPGPATH];
	char		portpath[MAXPGPATH];
	char		donepath[MAXPGPATH];
	char		tmppath[MAXPGPATH];
	FILE	   *f;
	uint64		bytes = 0;
	int			got = 0;

	if (pgdata == NULL)
		pg_fatal("PGDATA not set in the environment");
	if (nclients == 0)
		return 0;				/* this target receives from no source segment */

	lfd = listen_port(bindaddr, lo, hi, &port);

	snprintf(dir, sizeof(dir), "%s/gp_lr", pgdata);
	mkdir(dir, 0700);
	snprintf(portpath, sizeof(portpath), "%s/%s.%d.port", dir, job, seg);
	snprintf(tmppath, sizeof(tmppath), "%s.tmp", portpath);
	f = fopen(tmppath, "w");
	if (f == NULL)
		pg_fatal("could not write %s: %m", tmppath);
	fprintf(f, "%d\n", port);
	fclose(f);
	if (rename(tmppath, portpath) != 0)	/* atomic publish */
		pg_fatal("could not rename %s: %m", portpath);

	while (got < nclients)
	{
		fd_set		rfds;
		struct timeval tv;
		int			cfd;

		FD_ZERO(&rfds);
		FD_SET(lfd, &rfds);
		tv.tv_sec = LR_ACCEPT_TIMEOUT_S;
		tv.tv_usec = 0;
		if (select(lfd + 1, &rfds, NULL, NULL, &tv) <= 0)
			pg_fatal("timed out waiting for %d of %d source connections", nclients - got, nclients);
		cfd = accept(lfd, NULL, NULL);
		if (cfd < 0)
		{
			if (errno == EINTR)
				continue;
			pg_fatal("accept failed: %m");
		}
		if (!recv_one_client(cfd, &bytes))
			pg_fatal("source connection closed before the end marker");
		close(cfd);				/* signals the sender we are done */
		got++;
	}
	close(lfd);

	snprintf(donepath, sizeof(donepath), "%s/%s.%d.done", dir, job, seg);
	f = fopen(donepath, "w");
	if (f)
	{
		fprintf(f, "%llu\n", (unsigned long long) bytes);
		fclose(f);
	}
	unlink(portpath);
	pg_log_info("recv seg %d: %d clients, %llu bytes", seg, nclients,
				(unsigned long long) bytes);
	return 0;
}

/* ------------------------------------------------------------------------ */

int
main(int argc, char **argv)
{
	static struct option long_options[] = {
		{"job", required_argument, NULL, 'j'},
		{"seg", required_argument, NULL, 's'},
		{"clients", required_argument, NULL, 'n'},
		{"targets", required_argument, NULL, 't'},
		{"port-range", required_argument, NULL, 'r'},
		{"bindaddr", required_argument, NULL, 'b'},
		{"crc", no_argument, NULL, 'c'},
		{NULL, 0, NULL, 0}
	};
	const char *mode;
	char	   *job = NULL;
	int			seg = -1;
	int			nclients = -1;
	char	   *targets = NULL;
	char	   *bindaddr = NULL;
	int			lo = 42000,
				hi = 42999;
	int			c;

	pg_logging_init(argv[0]);

	if (argc < 2)
	{
		usage();
		return 1;
	}
	if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
	{
		usage();
		return 0;
	}
	mode = argv[1];

	while ((c = getopt_long(argc - 1, argv + 1, "j:s:n:t:r:b:c", long_options, NULL)) != -1)
	{
		switch (c)
		{
			case 'j': job = pg_strdup(optarg); break;
			case 's': seg = atoi(optarg); break;
			case 'n': nclients = atoi(optarg); break;
			case 't': targets = pg_strdup(optarg); break;
			case 'b': bindaddr = pg_strdup(optarg); break;
			case 'c': use_crc = true; break;
			case 'r':
				if (sscanf(optarg, "%d-%d", &lo, &hi) != 2)
					pg_fatal("bad --port-range \"%s\"", optarg);
				break;
			default:
				usage();
				return 1;
		}
	}

	if (job == NULL || seg < 0)
		pg_fatal("--job and --seg are required");

	if (strcmp(mode, "recv") == 0)
	{
		if (nclients < 0)
			pg_fatal("recv requires --clients");
		return do_recv(job, seg, nclients, lo, hi, bindaddr);
	}
	else if (strcmp(mode, "send") == 0)
	{
		if (targets == NULL)
			pg_fatal("send requires --targets");
		return do_send(job, seg, targets);
	}
	pg_fatal("unknown mode \"%s\" (expected recv or send)", mode);
	return 1;
}
