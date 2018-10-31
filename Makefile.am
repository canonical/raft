ACLOCAL_AMFLAGS = -I m4
AUTOMAKE_OPTIONS = foreign subdir-objects

lib_LTLIBRARIES =
check_PROGRAMS =
EXTRA_DIST =
noinst_HEADERS =
include_HEADERS =

EXTRA_DIST += raft.pc.in
pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = raft.pc

AM_CFLAGS += -I$(top_srcdir)/include -Wall
if DEBUG
  AM_CFLAGS += -DRAFT_ENABLE_DEBUG_LOGGING
else
  AM_CFLAGS += -Werror
endif
if COVERAGE
  AM_CFLAGS += -fprofile-arcs -ftest-coverage -DRAFT_COVERAGE
endif
if SANITIZE
  AM_CFLAGS += -fsanitize=address
endif
if EXPERIMENTAL
  AM_CFLAGS += -DRAFT_EXPERIMENTAL
endif

lib_LTLIBRARIES += libraft.la
libraft_la_LDFLAGS = -version-info 0:7:0
libraft_la_SOURCES = \
  src/client.c \
  src/configuration.c \
  src/context.c \
  src/election.c \
  src/encoding.c \
  src/error.c \
  src/heap.c \
  src/io.c \
  src/log.c \
  src/logger.c \
  src/raft.c \
  src/replication.c \
  src/rpc.c \
  src/state.c \
  src/tick.c
include_HEADERS += \
  include/raft.h

check_PROGRAMS += \
  unit-test \
  integration-test

test_lib_SOURCES = \
  test/lib/cluster.c \
  test/lib/fault.c \
  test/lib/heap.c \
  test/lib/io.c \
  test/lib/logger.c \
  test/lib/network.c \
  test/lib/munit.c \
  test/lib/raft.c

unit_test_SOURCES = $(test_lib_SOURCES)
unit_test_SOURCES += \
  test/unit/main.c \
  test/unit/test_client.c \
  test/unit/test_configuration.c \
  test/unit/test_election.c \
  test/unit/test_encoding.c \
  test/unit/test_error.c \
  test/unit/test_log.c \
  test/unit/test_logger.c \
  test/unit/test_context.c \
  test/unit/test_io.c \
  test/unit/test_raft.c \
  test/unit/test_replication.c \
  test/unit/test_rpc.c \
  test/unit/test_tick.c
unit_test_CFLAGS = $(AM_CFLAGS) -DMUNIT_TEST_NAME_LEN=47 -DMUNIT_NO_FORK
unit_test_CFLAGS += -I$(top_srcdir)/tests/unit
unit_test_LDADD = libraft.la
unit_test_LDFLAGS =

integration_test_SOURCES = $(test_lib_SOURCES)
integration_test_SOURCES += \
  test/integration/main.c \
  test/integration/test_election.c \
  test/integration/test_liveness.c \
  test/integration/test_replication.c
integration_test_CFLAGS = $(AM_CFLAGS) -DMUNIT_NO_FORK
integration_test_CFLAGS += -I$(top_srcdir)/tests/integration
integration_test_LDADD = libraft.la
integration_test_LDFLAGS =

TESTS = unit-test integration-test

COV_FLAGS = --rc lcov_branch_coverage=1 --rc lcov_excl_br_line="assert\("
COV_FLAGS += --compat-libtool --directory src --capture --output-file coverage/raft.info
if LLVM_COV
COV_FLAGS += --gcov-tool ./.gcov.sh
endif

cov-reset:
if COVERAGE
if LLVM_COV
	@find . -name "*.gcda" | xargs rm -f
else
	@lcov --directory src --zerocounters
endif
endif

cov-clean:
	@rm -fr coverage
	@find . -name "*.gcda" -o -name '*.gcno' | xargs rm -f

cov-report:
	@mkdir -p coverage
	@lcov $(COV_FLAGS)
	@genhtml --branch-coverage -o coverage/ coverage/raft.info

clean-local:
	@make cov-clean

check: cov-reset
if LLVM_COV
	@llvm-cov gcov -o ./src/.libs/ src/*.c
	@mv *.gcov src/
endif
if COVERAGE
	@make cov-report
endif
