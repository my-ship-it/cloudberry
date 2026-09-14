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
