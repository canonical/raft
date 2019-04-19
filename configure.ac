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

# Enable integration with libuv.
AC_ARG_ENABLE(uv,
  AS_HELP_STRING(
    [--enable-uv],
    [enable integration with libuv, default: yes]),
  [case "${enableval}" in
     yes) uv=true ;;
     no)  uv=false ;;
     *)   AC_MSG_ERROR([bad value ${enableval} for --enable-uv]) ;;
   esac],
  [uv=true])
AM_CONDITIONAL(UV, test x"$uv" = x"true")
AM_COND_IF(UV, [PKG_CHECK_MODULES(UV, [libuv >= 1.8.0], [], [])])

# Enable the fake I/O implementation and associated fixture, for testing.
AC_ARG_ENABLE(fixture,
  AS_HELP_STRING(
    [--enable-fixture],
    [enable raft test fixture, default: no]),
  [case "${enableval}" in
     yes) fixture=true ;;
     no)  fixture=false ;;
     *)   AC_MSG_ERROR([bad value ${enableval} for --enable-fixture]) ;;
   esac],
  [fixture=true])
AM_CONDITIONAL(FIXTURE, test x"$fixture" = x"true")

# Enable the example server.
AC_ARG_ENABLE(example,
  AS_HELP_STRING(
    [--enable-example],
    [enable example server, default: yes]),
  [case "${enableval}" in
     yes) example=true ;;
     no)  example=false ;;
     *)   AC_MSG_ERROR([bad value ${enableval} for --enable-example]) ;;
   esac],
  [example=true])
AM_CONDITIONAL(EXAMPLE, test x"$example" = x"true")

# Enable debugging code.
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

AX_CHECK_COMPILE_FLAG([-fdiagnostics-color], [CFLAGS="$CFLAGS -fdiagnostics-color"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Wimplicit-fallthrough=5], [CFLAGS="$CFLAGS -Wimplicit-fallthrough=5"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Wcast-align], [CFLAGS="$CFLAGS -Wcast-align"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Wstrict-prototypes], [CFLAGS="$CFLAGS -Wstrict-prototypes"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-fno-strict-aliasing], [CFLAGS="$CFLAGS -fno-strict-aliasing"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-fstack-clash-protection], [CFLAGS="$CFLAGS -fstack-clash-protection"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-fstack-protector-strong], [CFLAGS="$CFLAGS -fstack-protector-strong"],,[-Werror])
AX_CHECK_COMPILE_FLAG([--param=ssp-buffer-size=4], [CFLAGS="$CFLAGS --param=ssp-buffer-size=4"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-g], [CFLAGS="$CFLAGS -g"],,[-Werror])
AX_CHECK_COMPILE_FLAG([--mcet -fcf-protection], [CFLAGS="$CFLAGS --mcet -fcf-protection"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Werror=implicit-function-declaration], [CFLAGS="$CFLAGS -Werror=implicit-function-declaration"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Wlogical-op], [CFLAGS="$CFLAGS -Wlogical-op"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Wmissing-include-dirs], [CFLAGS="$CFLAGS -Wmissing-include-dirs"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Wold-style-definition], [CFLAGS="$CFLAGS -Wold-style-definition"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Winit-self], [CFLAGS="$CFLAGS -Winit-self"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Wfloat-equal], [CFLAGS="$CFLAGS -Wfloat-equal"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Wsuggest-attribute=noreturn], [CFLAGS="$CFLAGS -Wsuggest-attribute=noreturn"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Werror=return-type], [CFLAGS="$CFLAGS -Werror=return-type"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Werror=incompatible-pointer-types], [CFLAGS="$CFLAGS -Werror=incompatible-pointer-types"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Wformat=2], [CFLAGS="$CFLAGS -Wformat=2"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Wshadow], [CFLAGS="$CFLAGS -Wshadow"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Wendif-labels], [CFLAGS="$CFLAGS -Wendif-labels"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Werror=overflow], [CFLAGS="$CFLAGS -Werror=overflow"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-fdiagnostics-show-option], [CFLAGS="$CFLAGS -fdiagnostics-show-option"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Werror=shift-count-overflow], [CFLAGS="$CFLAGS -Werror=shift-count-overflow"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Werror=shift-overflow=2], [CFLAGS="$CFLAGS -Werror=shift-overflow=2"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Wdate-time], [CFLAGS="$CFLAGS -Wdate-time"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-Wnested-externs], [CFLAGS="$CFLAGS -Wnested-externs"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-fasynchronous-unwind-tables], [CFLAGS="$CFLAGS -fasynchronous-unwind-tables"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-pipe], [CFLAGS="$CFLAGS -pipe"],,[-Werror])
AX_CHECK_COMPILE_FLAG([-fexceptions], [CFLAGS="$CFLAGS -fexceptions"],,[-Werror])

AC_CONFIG_FILES([
  Makefile
  raft.pc
])
AC_OUTPUT
