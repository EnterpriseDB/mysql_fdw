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
PG_CPPFLAGS := $(shell $(MYSQL_CONFIG) --include)
LIB := $(shell $(MYSQL_CONFIG) --libs)

# In Debian based distros, libmariadbclient-dev provides mariadbclient (rather than mysqlclient)
ifneq ($(findstring mariadbclient,$(LIB)),)
MYSQL_LIB = mariadbclient
else
MYSQL_LIB = mysqlclient
endif

UNAME = uname
OS := $(shell $(UNAME))
ifeq ($(OS), Darwin)
DLSUFFIX = .dylib
else
DLSUFFIX = .so
endif

PG_CPPFLAGS += -D _MYSQL_LIBNAME=\"lib$(MYSQL_LIB)$(DLSUFFIX)\"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif
ifeq (,$(findstring $(MAJORVERSION), 9.3 9.4 9.5 9.6))
$(error PostgreSQL 9.3, 9.4, 9.5 or 9.6 is required to compile this extension)
endif

else
subdir = contrib/mysql_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

