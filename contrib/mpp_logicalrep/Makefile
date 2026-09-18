# contrib/mpp_logicalrep/Makefile

MODULE_big = mpp_logicalrep
OBJS = \
	$(WIN32RES) \
	logicalrep_apply_driver.o \
	mpp_apply_plan.o \
	mpp_apply_sql.o \
	mpp_apply_worker.o \
	mpp_logicalrep.o \
	mpp_reader.o \
	mpp_route.o \
	mpp_sink_cbdb.o \
	mpp_source.o \
	mpp_tablesync.o \
	mpp_writer.o
PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

PGFILEDESC = "mpp_logicalrep - logical replication apply into an MPP target"

# These run against a cluster that has the library preloaded -- the module is
# nothing without it, and a run against a cluster that has not got it would be
# green for the wrong reason.  installcheck-world has no cluster of its own to
# hand out, so rather than fail everybody else's run, test nothing and say
# why.  The probe happens only when a test run is being asked for, so an
# ordinary build never goes looking for a server.
REGRESS = subscription_options security

ifneq (,$(filter installcheck,$(MAKECMDGOALS)))
ifeq (,$(shell psql -X -At -d postgres -c "SHOW shared_preload_libraries" \
                2>/dev/null | grep mpp_logicalrep))
REGRESS =
MPP_NOT_CONFIGURED = yes
endif
endif

SUBDIRS = copyhelper

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/mpp_logicalrep
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# The copy transport helper is a standalone program (see mpp_tablesync.c).
all install installdirs uninstall clean distclean: %: %-copyhelper
%-copyhelper:
	$(MAKE) -C copyhelper $*

# The catalog objects go in through initdb, not CREATE EXTENSION: they must be
# coordinator-only tables, which an ordinary CREATE TABLE cannot produce.
install: install-cdbinit
install-cdbinit: installdirs-cdbinit
	$(INSTALL_DATA) $(srcdir)/mpp_logicalrep_cdbinit.sql '$(DESTDIR)$(datadir)/cdb_init.d/mpp_logicalrep_cdbinit.sql'

installdirs-cdbinit:
	$(MKDIR_P) '$(DESTDIR)$(datadir)/cdb_init.d'

uninstall: uninstall-cdbinit
uninstall-cdbinit:
	rm -f '$(DESTDIR)$(datadir)/cdb_init.d/mpp_logicalrep_cdbinit.sql'

# The timing-dependent tests are isolation2's: several sessions, one of them
# in the subscriber's database, and conditions waited on rather than slept
# through.  Its driver is built by src/test/isolation2, which an ordinary
# build does not descend into, so build it if it is not there.
ISOLATION2_DIR = $(top_builddir)/src/test/isolation2
ISOLATION2 = $(ISOLATION2_DIR)/pg_isolation2_regress$(X)

installcheck: installcheck-isolation2
.PHONY: installcheck-isolation2
installcheck-isolation2:
ifndef MPP_NOT_CONFIGURED
	@test -x $(ISOLATION2) || $(MAKE) -C $(ISOLATION2_DIR) pg_isolation2_regress$(X)
	@# the driver shells out to this next to the tests; borrow it rather
	@# than keep a copy of it here
	@rm -f $(srcdir)/isolation2/sql_isolation_testcase.py
	@$(LN_S) $(abs_top_srcdir)/src/test/isolation2/sql_isolation_testcase.py $(srcdir)/isolation2/sql_isolation_testcase.py
	cd $(srcdir)/isolation2 && $(abs_top_builddir)/src/test/isolation2/pg_isolation2_regress 		--bindir='$(bindir)' 		--init-file=$(abs_top_srcdir)/src/test/regress/init_file 		--init-file=$(abs_top_srcdir)/src/test/isolation2/init_file_isolation2 		--dbname=mpp_lr_source 		--schedule=./mpp_logicalrep_schedule
endif

ifdef MPP_NOT_CONFIGURED
installcheck: say-not-run
.PHONY: say-not-run
say-not-run:
	@echo "============================================================"
	@echo "  contrib/mpp_logicalrep: NOT RUN"
	@echo ""
	@echo "  The cluster at PGPORT=$${PGPORT-<default>} does not preload"
	@echo "  mpp_logicalrep, so there is nothing here to test."
	@echo ""
	@echo "  Build one that does:"
	@echo "    cd gpAux/gpdemo && \\"
	@echo "      EXTRA_CONFIG=\"shared_preload_libraries='mpp_logicalrep'\" \\"
	@echo "      ./demo_cluster.sh"
	@echo "  and set wal_level = logical on it."
	@echo "  See contrib/mpp_logicalrep/README, Installation."
	@echo "============================================================"
endif
