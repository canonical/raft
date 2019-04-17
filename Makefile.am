ACLOCAL_AMFLAGS = -I m4
AUTOMAKE_OPTIONS = foreign subdir-objects

lib_LTLIBRARIES =
bin_PROGRAMS =
check_PROGRAMS =
EXTRA_DIST =
noinst_HEADERS =
include_HEADERS =

EXTRA_DIST += raft.pc.in
pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = raft.pc

AM_CFLAGS += -I$(top_srcdir)/include -Wall
if UV
  AM_CFLAGS += $(UV_CFLAGS)
endif
if DEBUG
  AM_CFLAGS +=
else
#  AM_CFLAGS += -Werror
  AM_CFLAGS +=
endif
if COVERAGE
  AM_CFLAGS += -fprofile-arcs -ftest-coverage
endif
if SANITIZE
  AM_CFLAGS += -fsanitize=address
endif

lib_LTLIBRARIES += libraft.la
libraft_la_LDFLAGS = -version-info 0:7:0
if UV
  libraft_la_LDFLAGS += $(UV_LIBS)
endif
libraft_la_SOURCES = \
  src/aio.c \
  src/byte.c \
  src/client.c \
  src/configuration.c \
  src/convert.c \
  src/election.c \
  src/entry.c \
  src/error.c \
  src/heap.c \
  src/heartbeat.c \
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
if UV
  libraft_la_SOURCES += \
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
endif
if FIXTURE
  libraft_la_SOURCES += \
  src/io_stub.c \
  src/fixture.c
endif
include_HEADERS += \
  include/raft.h
pkginclude_HEADERS =
if FIXTURE
  pkginclude_HEADERS += \
  include/raft/io_stub.h \
  include/raft/fixture.h
endif
if UV
  pkginclude_HEADERS += \
  include/raft/uv.h
endif

check_PROGRAMS += \
  unit-test \
  fuzzy-test

test_lib_SOURCES = \
  test/lib/fault.c \
  test/lib/fs.c \
  test/lib/fsm.c \
  test/lib/heap.c \
  test/lib/io.c \
  test/lib/munit.c \
  test/lib/tcp.c
if UV
test_lib_SOURCES += \
  test/lib/uv.c \
  test/lib/loop.c
endif

unit_test_SOURCES = $(libraft_la_SOURCES)
unit_test_SOURCES += \
  $(test_lib_SOURCES)
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
  test/unit/test_start.c \
  test/unit/test_tick.c
if UV
  unit_test_SOURCES += \
  test/unit/test_os.c \
  test/unit/test_uv.c \
  test/unit/test_uv_append.c \
  test/unit/test_uv_file.c \
  test/unit/test_uv_finalize.c \
  test/unit/test_uv_list.c \
  test/unit/test_uv_load.c \
  test/unit/test_uv_metadata.c \
  test/unit/test_uv_recv.c \
  test/unit/test_uv_prepare.c \
  test/unit/test_uv_send.c \
  test/unit/test_uv_snapshot.c \
  test/unit/test_uv_tcp_listen.c \
  test/unit/test_uv_tcp_connect.c \
  test/unit/test_uv_truncate.c
endif
if FIXTURE
  unit_test_SOURCES += \
  test/unit/test_fixture.c
endif
unit_test_CFLAGS = $(AM_CFLAGS) -DRAFT_TEST -DMUNIT_TEST_NAME_LEN=60
unit_test_LDFLAGS =
if UV
  unit_test_LDFLAGS += $(UV_LIBS)
endif

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
if UV
  fuzzy_test_LDFLAGS += $(UV_LIBS)
endif

TESTS = unit-test fuzzy-test

COV_FLAGS = --rc genhtml_branch_coverage=1 --rc lcov_branch_coverage=1 --rc lcov_excl_br_line="assert\("
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

if EXAMPLE

bin_PROGRAMS += example-server
example_server_SOURCES = \
  example/server.c
example_server_CFLAGS = $(AM_CFLAGS)
example_server_CFLAGS += -I$(top_srcdir)/example
example_server_LDADD = libraft.la
example_server_LDFLAGS = $(UV_LIBS)

bin_PROGRAMS += example-cluster
example_cluster_SOURCES = \
  example/cluster.c
example_cluster_CFLAGS = $(AM_CFLAGS)
example_cluster_CFLAGS += -I$(top_srcdir)/example
example_cluster_LDADD = libraft.la
example_cluster_LDFLAGS = $(UV_LIBS)

endif
