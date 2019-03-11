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
if IO_UV
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
if IO_UV
  libraft_la_LDFLAGS += $(UV_LIBS)
endif
libraft_la_SOURCES = \
  src/aio.c \
  src/byte.c \
  src/client.c \
  src/configuration.c \
  src/election.c \
  src/error.c \
  src/heap.c \
  src/log.c \
  src/logger.c \
  src/membership.c \
  src/raft.c \
  src/replication.c \
  src/rpc.c \
  src/rpc_append_entries.c \
  src/rpc_request_vote.c \
  src/rpc_install_snapshot.c \
  src/snapshot.c \
  src/state.c \
  src/tick.c \
  src/watch.c
if IO_UV
  libraft_la_SOURCES += \
  src/io_uv.c \
  src/io_uv_append.c \
  src/io_uv_client.c \
  src/io_uv_encoding.c \
  src/io_uv_finalize.c \
  src/io_uv_fs.c \
  src/io_uv_ip.c \
  src/io_uv_load.c \
  src/io_uv_metadata.c \
  src/io_uv_prepare.c \
  src/io_uv_server.c \
  src/io_uv_snapshot.c \
  src/io_uv_tcp.c \
  src/io_uv_tcp_connect.c \
  src/io_uv_tcp_listen.c \
  src/io_uv_truncate.c \
  src/uv_file.c
endif
if IO_STUB
  libraft_la_SOURCES += \
  src/io_stub.c
endif
include_HEADERS += \
  include/raft.h

check_PROGRAMS += \
  unit-test \
  integration-test

test_lib_SOURCES = \
  test/lib/cluster.c \
  test/lib/fault.c \
  test/lib/fs.c \
  test/lib/fsm.c \
  test/lib/heap.c \
  test/lib/io.c \
  test/lib/logger.c \
  test/lib/munit.c \
  test/lib/network.c \
  test/lib/raft.c \
  test/lib/tcp.c
if IO_UV
test_lib_SOURCES += \
  test/lib/io_uv.c \
  test/lib/uv.c
endif

unit_test_SOURCES = $(libraft_la_SOURCES)
unit_test_SOURCES += \
  $(test_lib_SOURCES)
unit_test_SOURCES += \
  test/unit/main.c \
  test/unit/test_byte.c \
  test/unit/test_client.c \
  test/unit/test_configuration.c \
  test/unit/test_election.c \
  test/unit/test_log.c \
  test/unit/test_logger.c \
  test/unit/test_queue.c \
  test/unit/test_raft.c \
  test/unit/test_replication.c \
  test/unit/test_rpc_append_entries.c \
  test/unit/test_rpc_request_vote.c \
  test/unit/test_rpc_install_snapshot.c \
  test/unit/test_tick.c
if IO_UV
  unit_test_SOURCES += \
  test/unit/test_io_uv.c \
  test/unit/test_io_uv_append.c \
  test/unit/test_io_uv_client.c \
  test/unit/test_io_uv_finalize.c \
  test/unit/test_io_uv_load.c \
  test/unit/test_io_uv_metadata.c \
  test/unit/test_io_uv_prepare.c \
  test/unit/test_io_uv_server.c \
  test/unit/test_io_uv_snapshot.c \
  test/unit/test_io_uv_tcp.c \
  test/unit/test_io_uv_truncate.c \
  test/unit/test_uv_file.c
endif
if IO_STUB
  unit_test_SOURCES += \
  test/unit/test_io_stub.c
endif
unit_test_CFLAGS = $(AM_CFLAGS) -DRAFT_TEST -DMUNIT_TEST_NAME_LEN=60
unit_test_CFLAGS += -I$(top_srcdir)/tests/unit
unit_test_LDFLAGS =
if IO_UV
  unit_test_LDFLAGS += $(UV_LIBS)
endif

integration_test_SOURCES = $(test_lib_SOURCES)
integration_test_SOURCES += \
  test/integration/main.c \
  test/integration/test_election.c \
  test/integration/test_liveness.c \
  test/integration/test_membership.c \
  test/integration/test_replication.c
integration_test_CFLAGS = $(AM_CFLAGS)
#-DMUNIT_NO_FORK
integration_test_CFLAGS += -I$(top_srcdir)/tests/integration
integration_test_LDADD = libraft.la
integration_test_LDFLAGS =
if IO_UV
  integration_test_LDFLAGS += $(UV_LIBS)
endif

TESTS = unit-test integration-test

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
