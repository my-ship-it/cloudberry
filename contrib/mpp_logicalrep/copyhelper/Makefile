# contrib/mpp_logicalrep/copyhelper/Makefile

PGFILEDESC = "cbdb_lr_copyhelper - MPP logical replication segment-to-segment copy transport"
PGAPPICON = win32

PROGRAM = cbdb_lr_copyhelper
OBJS = \
	$(WIN32RES) \
	cbdb_lr_copyhelper.o

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/mpp_logicalrep/copyhelper
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
