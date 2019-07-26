ACLOCAL_AMFLAGS = -I m4
AM_CFLAGS = -I$(top_srcdir)/include -Wall

if SANITIZE_ENABLED
  AM_CFLAGS += -fsanitize=address
endif

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
  src/tick.c

check_PROGRAMS = unit-test integration-test fuzzy-test
TESTS = $(check_PROGRAMS)

test_CFLAGS = $(AM_CFLAGS) -DRAFT_TEST -DMUNIT_TEST_NAME_LEN=60
test_lib_SOURCES = \
  test/lib/cluster.c \
  test/lib/fault.c \
  test/lib/fsm.c \
  test/lib/heap.c \
  test/lib/munit.c \
  test/lib/tcp.c

unit_test_SOURCES = $(test_lib_SOURCES)
unit_test_SOURCES += $(libraft_la_SOURCES)
unit_test_SOURCES += \
  test/unit/main.c \
  test/unit/test_apply.c \
  test/unit/test_barrier.c \
  test/unit/test_byte.c \
  test/unit/test_configuration.c \
  test/unit/test_election.c \
  test/unit/test_log.c \
  test/unit/test_membership.c \
  test/unit/test_queue.c \
  test/unit/test_replication.c \
  test/unit/test_snapshot.c \
  test/unit/test_tick.c
unit_test_CFLAGS = $(CODE_COVERAGE_CFLAGS) $(test_CFLAGS)
unit_test_LDADD =
unit_test_LDFLAGS =

integration_test_SOURCES = $(test_lib_SOURCES)
integration_test_SOURCES += \
  test/integration/main.c \
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

libraft_la_SOURCES += \
  src/aio.c \
  src/os.c \
  src/uv.c \
  src/uv_append.c \
  src/uv_encoding.c \
  src/uv_file.c \
  src/uv_finalize.c \
  src/uv_ip.c \
  src/uv_list.c \
  src/uv_metadata.c \
  src/uv_prepare.c \
  src/uv_recv.c \
  src/uv_segment.c \
  src/uv_send.c \
  src/uv_snapshot.c \
  src/uv_tcp.c \
  src/uv_tcp_connect.c \
  src/uv_tcp_listen.c \
  src/uv_truncate.c
libraft_la_LDFLAGS += $(UV_LIBS)

raftinclude_HEADERS += include/raft/uv.h

test_lib_SOURCES += \
  test/lib/loop.c

unit_test_SOURCES += \
  test/lib/fs.c \
  test/lib/uv.c \
  test/unit/test_os.c \
  test/unit/test_uv.c \
  test/unit/test_uv_append.c \
  test/unit/test_uv_file.c \
  test/unit/test_uv_finalize.c \
  test/unit/test_uv_list.c \
  test/unit/test_uv_load.c \
  test/unit/test_uv_metadata.c \
  test/unit/test_uv_prepare.c \
  test/unit/test_uv_recv.c \
  test/unit/test_uv_send.c \
  test/unit/test_uv_snapshot.c \
  test/unit/test_uv_tcp_connect.c \
  test/unit/test_uv_tcp_listen.c \
  test/unit/test_uv_truncate.c
unit_test_LDFLAGS += $(UV_LIBS)

AM_CFLAGS += $(UV_CFLAGS)

endif # UV_ENABLED

if FIXTURE_ENABLED
libraft_la_SOURCES += src/fixture.c
raftinclude_HEADERS += include/raft/fixture.h
unit_test_SOURCES += test/unit/test_fixture.c
endif

if EXAMPLE_ENABLED

bin_PROGRAMS = \
 example-server \
 example-cluster

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

CODE_COVERAGE_DIRECTORY=./src
CODE_COVERAGE_OUTPUT_DIRECTORY=coverage
CODE_COVERAGE_OUTPUT_FILE=coverage.info
CODE_COVERAGE_IGNORE_PATTERN=/usr/include
CODE_COVERAGE_BRANCH_COVERAGE=1
CODE_COVERAGE_LCOV_OPTIONS=$(CODE_COVERAGE_LCOV_OPTIONS_DEFAULT) --rc lcov_excl_br_line="assert\("

include $(top_srcdir)/aminclude_static.am

clean-local: code-coverage-clean
distclean-local: code-coverage-dist-clean

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = @PACKAGE_NAME@.pc
