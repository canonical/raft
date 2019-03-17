AC_PREREQ(2.60)

AC_INIT([raft], [0.7.0], [https://github.com/CanonicalLtd/raft/issues])
AC_CONFIG_SRCDIR(include/raft.h)
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_AUX_DIR([ac])
AC_CANONICAL_HOST
AC_USE_SYSTEM_EXTENSIONS
AC_LANG([C])
AC_PROG_CC

#AM_INIT_AUTOMAKE([subdir-objects -Wall -Werror foreign])
AM_INIT_AUTOMAKE([subdir-objects -Wall foreign])
AM_SILENT_RULES([yes])

LT_INIT

AC_PROG_INSTALL
AC_PROG_LN_S

# TODO: eventually enable this
# AX_CHECK_COMPILE_FLAG([-Weverything], AM_CFLAGS+=" -Weverything")

# Enable building the I/O implementation based on libuv.
AC_ARG_ENABLE(io-uv,
  AS_HELP_STRING(
    [--enable-io-uv],
    [enable I/O implementation based on libuv, default: no]),
  [case "${enableval}" in
     yes) io_uv=true ;;
     no)  io_uv=false ;;
     *)   AC_MSG_ERROR([bad value ${enableval} for --enable-io-uv]) ;;
   esac],
  [io_uv=false])
AM_CONDITIONAL(IO_UV, test x"$io_uv" = x"true")
AM_COND_IF(IO_UV,
  [PKG_CHECK_MODULES(UV, [libuv >= 1.8.0], [], [])
   AC_DEFINE(RAFT_IO_UV)])

# Enable building the stub I/O implementation, for testing.
AC_ARG_ENABLE(io-stub,
  AS_HELP_STRING(
    [--enable-io-stub],
    [enable the stub I/O implementation, default: no]),
  [case "${enableval}" in
     yes) io_stub=true ;;
     no)  io_stub=false ;;
     *)   AC_MSG_ERROR([bad value ${enableval} for --enable-io-stub]) ;;
   esac],
  [io_stub=true])
AM_CONDITIONAL(IO_STUB, test x"$io_stub" = x"true")
AM_COND_IF(IO_STUB, AC_DEFINE(RAFT_IO_STUB))

# Enable building the example server.
AC_ARG_ENABLE(example,
  AS_HELP_STRING(
    [--enable-example],
    [enable building example server, default: no]),
  [case "${enableval}" in
     yes) example=true ;;
     no)  example=false ;;
     *)   AC_MSG_ERROR([bad value ${enableval} for --enable-example]) ;;
   esac],
  [example=false])
AM_CONDITIONAL(EXAMPLE, test x"$example" = x"true")

# Enable debugging output.
AC_ARG_ENABLE(debug,
  AS_HELP_STRING(
    [--enable-debug],
    [enable debugging, default: no]),
  [case "${enableval}" in
     yes) debug=true ;;
     no)  debug=false ;;
     *)   AC_MSG_ERROR([bad value ${enableval} for --enable-debug]) ;;
   esac],
  [debug=false])
AM_CONDITIONAL(DEBUG, test x"$debug" = x"true")
AM_COND_IF(DEBUG, AC_DEFINE(RAFT_ENABLE_DEBUG_LOGGING))

# Enable test coverage.
AC_ARG_ENABLE(coverage,
  AS_HELP_STRING(
    [--enable-coverage],
    [enable code coverage, default: no]),
  [case "${enableval}" in
     yes) coverage=true ;;
     no)  coverage=false ;;
     *)   AC_MSG_ERROR([bad value ${enableval} for --enable-coverage]) ;;
   esac],
  [coverage=false])
AM_CONDITIONAL(COVERAGE, test x"$coverage" = x"true")
AM_COND_IF(COVERAGE,
  [AC_DEFINE(RAFT_COVERAGE)
   # Remove all optimization flags from CFLAGS
   changequote({,})
   CFLAGS=`echo "$CFLAGS" | $SED -e 's/-O[0-9]*//g'`
   changequote([,])])

# Enable memory sanitizer.
AC_ARG_ENABLE(sanitize,
  AS_HELP_STRING(
    [--enable-sanitize],
    [enable compiler sanitize instrumentation (needs clang), default: no]),
  [case "${enableval}" in
     yes) sanitize=true ;;
     no)  sanitize=false ;;
     *)   AC_MSG_ERROR([bad value ${enableval} for --sanitize-debug]) ;;
   esac],
  [sanitize=false])
AM_CONDITIONAL(SANITIZE, test x"$sanitize" = x"true")
AM_COND_IF(SANITIZE,
  AX_CHECK_COMPILE_FLAG([-fsanitize=address],
   [true],
   [AC_MSG_ERROR([address sanitizer not supported, clang only])]))

# When using Clang, we need to use llvm-cov to generate .gcov files
AX_CHECK_COMPILE_FLAG([-fprofile-instr-generate],
  [llvm_cov=true],
  [llvm_cov=false])
AM_CONDITIONAL(LLVM_COV, test x"$llvm_cov" = x"true")

AC_SUBST([AM_CFLAGS])

# Checks for header files.
AC_CHECK_HEADERS([stdlib.h string.h stdio.h assert.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_CHECK_FUNCS([memcpy vsprintf])

AC_CONFIG_FILES([
  Makefile
  raft.pc
])
AC_OUTPUT
