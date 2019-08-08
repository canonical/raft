ACLOCAL_AMFLAGS = -I m4
AM_CFLAGS = -I$(top_srcdir)/include -Wall

include_HEADERS = include/raft.h

raftincludedir = $(includedir)/raft
raftinclude_HEADERS =

lib_LTLIBRARIES = libraft.la
libraft_la_CFLAGS = -fvisibility=hidden $(CODE_COVERAGE_CFLAGS)
libraft_la_LDFLAGS = -version-info 0:7:0
libraft_la_SOURCES = \
  src/byte.c \
  src/client.c \
  src/configuration.c \
  src/convert.c \
  src/election.c \
  src/entry.c \
  src/error.c \
  src/heap.c \
  src/log.c \
  src/logging.c \
  src/membership.c \
  src/progress.c \
  src/raft.c \
  src/recv.c \
  src/recv_append_entries.c \
  src/recv_append_entries_result.c \
  src/recv_request_vote.c \
  src/recv_request_vote_result.c \
  src/recv_install_snapshot.c \
  src/replication.c \
  src/snapshot.c \
  src/start.c \
  src/state.c \
  src/tick.c \
  src/tracer.c

check_PROGRAMS = test/unit/core integration-test fuzzy-test
TESTS = $(check_PROGRAMS)

check_LTLIBRARIES = libtest.la

libtest_la_CFLAGS = -DMUNIT_TEST_NAME_LEN=60
libtest_la_LDFLAGS =
libtest_la_SOURCES = \
  test/lib/fault.c \
  test/lib/heap.c \
  test/lib/munit.c

test_CFLAGS = $(AM_CFLAGS) -DRAFT_TEST

test_unit_core_SOURCES = \
  src/byte.c \
  src/configuration.c \
  src/error.c \
  src/heap.c \
  src/log.c \
  src/tracer.c \
  test/unit/main_core.c \
  test/unit/test_byte.c \
  test/unit/test_configuration.c \
  test/unit/test_heap.c \
  test/unit/test_error.c \
  test/unit/test_log.c \
  test/unit/test_queue.c \
  test/unit/test_tracer.c
test_unit_core_CFLAGS = $(CODE_COVERAGE_CFLAGS) $(test_CFLAGS)
test_unit_core_LDADD = libtest.la

test_lib_SOURCES = \
  test/lib/cluster.c \
  test/lib/fault.c \
  test/lib/fsm.c \
  test/lib/heap.c \
  test/lib/munit.c \
  test/lib/tcp.c

integration_test_SOURCES = $(test_lib_SOURCES)
integration_test_SOURCES += \
  test/integration/main.c \
  test/integration/test_apply.c \
  test/integration/test_barrier.c \
  test/integration/test_election.c \
  test/integration/test_membership.c \
  test/integration/test_replication.c \
  test/integration/test_snapshot.c \
  test/integration/test_tick.c \
  test/integration/test_start.c
integration_test_CFLAGS = $(test_CFLAGS)
integration_test_LDADD = libraft.la
integration_test_LDFLAGS =

fuzzy_test_SOURCES = $(test_lib_SOURCES)
fuzzy_test_SOURCES += \
  test/fuzzy/main.c \
  test/fuzzy/test_election.c \
  test/fuzzy/test_liveness.c \
  test/fuzzy/test_membership.c \
  test/fuzzy/test_replication.c
fuzzy_test_CFLAGS = $(AM_CFLAGS)
fuzzy_test_LDADD = libraft.la
fuzzy_test_LDFLAGS =

if UV_ENABLED

raft_uv_SOURCES = \
  src/uv.c \
  src/uv_append.c \
  src/uv_encoding.c \
  src/uv_file.c \
  src/uv_finalize.c \
  src/uv_ip.c \
  src/uv_list.c \
  src/uv_metadata.c \
  src/uv_os.c \
  src/uv_prepare.c \
  src/uv_recv.c \
  src/uv_segment.c \
  src/uv_send.c \
  src/uv_snapshot.c \
  src/uv_tcp.c \
  src/uv_tcp_connect.c \
  src/uv_tcp_listen.c \
  src/uv_truncate.c

libraft_la_SOURCES += $(raft_uv_SOURCES)
libraft_la_LDFLAGS += $(UV_LIBS)

raftinclude_HEADERS += include/raft/uv.h

check_PROGRAMS += test/unit/uv

libtest_la_SOURCES += \
  test/lib/dir.c \
  test/lib/tcp.c \
  test/lib/loop.c \
  test/lib/uv.c

test_lib_SOURCES += \
  test/lib/loop.c

test_unit_uv_SOURCES = $(raft_uv_SOURCES)
test_unit_uv_SOURCES += \
  src/byte.c \
  src/configuration.c \
  src/logging.c \
  src/entry.c \
  src/error.c \
  src/heap.c \
  src/snapshot.c \
  src/tracer.c \
  test/unit/main_uv.c \
  test/unit/test_uv_file.c \
  test/unit/test_uv_metadata.c \
  test/unit/test_uv_os.c
test_unit_uv_CFLAGS = $(CODE_COVERAGE_CFLAGS) $(test_CFLAGS)
test_unit_uv_LDADD = libtest.la
test_unit_uv_LDFLAGS = $(UV_LIBS)

# unit_test_SOURCES += \
#   test/lib/dir.c \
#   test/lib/uv.c \
#   test/unit/test_uv.c \
#   test/unit/test_uv_append.c \
#   test/unit/test_uv_finalize.c \
#   test/unit/test_uv_list.c \
#   test/unit/test_uv_load.c \
#   test/unit/test_uv_metadata.c \
#   test/unit/test_uv_os.c \
#   test/unit/test_uv_prepare.c \
#   test/unit/test_uv_recv.c \
#   test/unit/test_uv_send.c \
#   test/unit/test_uv_snapshot.c \
#   test/unit/test_uv_tcp_connect.c \
#   test/unit/test_uv_tcp_listen.c \
#   test/unit/test_uv_truncate.c
# unit_test_LDFLAGS += $(UV_LIBS)

AM_CFLAGS += $(UV_CFLAGS)

endif # UV_ENABLED

if FIXTURE_ENABLED
libraft_la_SOURCES += src/fixture.c
raftinclude_HEADERS += include/raft/fixture.h
#unit_test_SOURCES += test/unit/test_fixture.c
endif

if EXAMPLE_ENABLED

bin_PROGRAMS = \
 example/server \
 example/cluster

example_server_SOURCES = example/server.c
example_server_CFLAGS = $(AM_CFLAGS)
example_server_CFLAGS += -I$(top_srcdir)/example
example_server_LDADD = libraft.la
example_server_LDFLAGS = $(UV_LIBS)

example_cluster_SOURCES = example/cluster.c
example_cluster_CFLAGS = $(AM_CFLAGS)
example_cluster_CFLAGS += -I$(top_srcdir)/example
example_cluster_LDADD = libraft.la
example_cluster_LDFLAGS = $(UV_LIBS)

endif # EXAMPLE_ENABLED

if SANITIZE_ENABLED
  AM_CFLAGS += -fsanitize=address
endif

if CODE_COVERAGE_ENABLED

include $(top_srcdir)/aminclude_static.am

CODE_COVERAGE_DIRECTORY=./src
CODE_COVERAGE_OUTPUT_DIRECTORY=coverage
CODE_COVERAGE_OUTPUT_FILE=coverage.info
CODE_COVERAGE_IGNORE_PATTERN="/usr/include/*"
CODE_COVERAGE_BRANCH_COVERAGE=1
CODE_COVERAGE_LCOV_OPTIONS=$(CODE_COVERAGE_LCOV_OPTIONS_DEFAULT) --rc lcov_excl_br_line="assert\("

clean-local: code-coverage-clean
distclean-local: code-coverage-dist-clean

endif # CODE_COVERAGE_ENABLED

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = @PACKAGE_NAME@.pc
