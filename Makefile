######################################################################-------------------------------------------------------------------------
#
# mysql_fdw.c
# 		Foreign-data wrapper for remote MySQL servers
#
# Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
#
# Portions Copyright (c) 2004-2014, EnterpriseDB Corporation.
#
# IDENTIFICATION
# 		mysql_fdw.c
#
##########################################################################

MODULE_big = mysql_fdw
OBJS = connection.o option.o deparse.o mysql_query.o mysql_fdw.o

EXTENSION = mysql_fdw
DATA = mysql_fdw--1.0.sql

REGRESS = mysql_fdw

MYSQL_CONFIG = mysql_config
SHLIB_LINK := $(shell $(MYSQL_CONFIG) --libs)
PG_CPPFLAGS := $(shell $(MYSQL_CONFIG) --include)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif
ifeq (,$(findstring $(MAJORVERSION), 9.3 9.4 9.5))
$(error PostgreSQL 9.3, 9.4 or 9.5 is required to compile this extension)
endif

else
subdir = contrib/mysql_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

