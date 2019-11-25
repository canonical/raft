ACLOCAL_AMFLAGS = -I m4
AM_CFLAGS += $(CODE_COVERAGE_CFLAGS)

include_HEADERS = include/raft.h

raftincludedir = $(includedir)/raft
raftinclude_HEADERS =

lib_LTLIBRARIES = libraft.la
libraft_la_CFLAGS = $(AM_CFLAGS) -fvisibility=hidden
libraft_la_LDFLAGS = -version-info 0:7:0
libraft_la_SOURCES = \
  src/byte.c \
  src/client.c \
  src/configuration.c \
  src/convert.c \
  src/election.c \
  src/entry.c \
  src/err.c \
  src/heap.c \
  src/log.c \
  src/logger_ring.c \
  src/logger_stream.c \
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
  src/syscall.c \
  src/tick.c \
  src/tracing.c

bin_PROGRAMS =

check_PROGRAMS = \
  test/unit/core \
  test/integration/core \
  fuzzy-test
TESTS = $(check_PROGRAMS)

check_LTLIBRARIES = libtest.la

libtest_la_CFLAGS = $(AM_CFLAGS) -DMUNIT_TEST_NAME_LEN=60
libtest_la_SOURCES = \
  test/lib/fault.c \
  test/lib/fsm.c \
  test/lib/heap.c \
  test/lib/munit.c \
  test/lib/tracer.c \
  test/lib/tcp.c

test_unit_core_SOURCES = \
  src/byte.c \
  src/configuration.c \
  src/err.c \
  src/heap.c \
  src/log.c \
  src/logger_ring.c \
  test/unit/main_core.c \
  test/unit/test_byte.c \
  test/unit/test_configuration.c \
  test/unit/test_err.c \
  test/unit/test_heap.c \
  test/unit/test_log.c \
  test/unit/test_queue.c \
  test/unit/test_logger_ring.c
test_unit_core_LDADD = libtest.la

test_integration_core_SOURCES = \
  test/integration/main_core.c \
  test/integration/test_apply.c \
  test/integration/test_barrier.c \
  test/integration/test_bootstrap.c \
  test/integration/test_election.c \
  test/integration/test_membership.c \
  test/integration/test_recover.c \
  test/integration/test_replication.c \
  test/integration/test_snapshot.c \
  test/integration/test_strerror.c \
  test/integration/test_tick.c \
  test/integration/test_start.c
test_integration_core_LDADD = libtest.la libraft.la
test_integration_core_LDFLAGS = -no-install

fuzzy_test_SOURCES = \
  test/lib/cluster.c \
  test/fuzzy/main.c \
  test/fuzzy/test_election.c \
  test/fuzzy/test_liveness.c \
  test/fuzzy/test_membership.c \
  test/fuzzy/test_replication.c
fuzzy_test_LDADD = libtest.la libraft.la
fuzzy_test_LDFLAGS = -no-install

if UV_ENABLED

raft_uv_SOURCES = \
  src/uv.c \
  src/uv_append.c \
  src/uv_encoding.c \
  src/uv_finalize.c \
  src/uv_fs.c \
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
  src/uv_tcp_listen.c \
  src/uv_tcp_connect.c \
  src/uv_truncate.c \
  src/uv_writer.c

libraft_la_SOURCES += $(raft_uv_SOURCES)
libraft_la_LDFLAGS += $(UV_LIBS)

raftinclude_HEADERS += include/raft/uv.h

check_PROGRAMS += \
  test/unit/uv \
  test/integration/uv

libtest_la_SOURCES += \
  test/lib/dir.c \
  test/lib/tcp.c \
  test/lib/loop.c

test_unit_uv_SOURCES = $(raft_uv_SOURCES)
test_unit_uv_SOURCES += \
  src/byte.c \
  src/configuration.c \
  src/tracing.c \
  src/entry.c \
  src/err.c \
  src/heap.c \
  src/snapshot.c \
  src/syscall.c \
  test/unit/main_uv.c \
  test/unit/test_uv_fs.c \
  test/unit/test_uv_list.c \
  test/unit/test_uv_recv.c \
  test/unit/test_uv_writer.c
test_unit_uv_LDADD = libtest.la
test_unit_uv_LDFLAGS = $(UV_LIBS)

test_integration_uv_SOURCES = \
  test/integration/main_uv.c \
  test/integration/test_uv_init.c \
  test/integration/test_uv_append.c \
  test/integration/test_uv_bootstrap.c \
  test/integration/test_uv_load.c \
  test/integration/test_uv_recover.c \
  test/integration/test_uv_send.c \
  test/integration/test_uv_set_term.c \
  test/integration/test_uv_tcp_connect.c \
  test/integration/test_uv_tcp_listen.c \
  test/integration/test_uv_snapshot_put.c \
  test/integration/test_uv_truncate.c
test_integration_uv_LDADD = libtest.la libraft.la
test_integration_uv_LDFLAGS = -no-install $(UV_LIBS)

AM_CFLAGS += $(UV_CFLAGS)

endif # UV_ENABLED

if FIXTURE_ENABLED
libraft_la_SOURCES += src/fixture.c
raftinclude_HEADERS += include/raft/fixture.h
test_integration_core_SOURCES += test/unit/test_fixture.c
endif

if EXAMPLE_ENABLED

bin_PROGRAMS += \
 example/server \
 example/cluster

example_server_SOURCES = example/server.c
example_server_LDADD = libraft.la
example_server_LDFLAGS = -no-install $(UV_LIBS)

example_cluster_SOURCES = example/cluster.c

endif # EXAMPLE_ENABLED

if BENCHMARK_ENABLED

bin_PROGRAMS += \
 benchmark/os-disk-write

benchmark_os_disk_write_SOURCES = benchmark/os_disk_write.c
benchmark_os_disk_write_LDFLAGS = -luring

endif # BENCHMARK_ENABLED

if !DEBUG_ENABLED
  AM_CFLAGS += -DNDEBUG
endif

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
