##########################################################################
#
#                foreign-data wrapper for MySQL
#
# Copyright (c) 2011 - 2013, PostgreSQL Global Development Group
#
# This software is released under the PostgreSQL Licence
#
# Author: Dave Page <dpage@pgadmin.org>
#
# IDENTIFICATION
#                 mysql_fdw/Makefile
# 
##########################################################################

MODULE_big = mysql_fdw
OBJS = option.o mysql_fdw.o

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
else
subdir = contrib/mysql_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

