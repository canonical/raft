AC_PREREQ(2.60)

AC_INIT(raft, 0.7.0, [])
AC_CONFIG_SRCDIR(include/raft.h)
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_AUX_DIR([ac])

AC_CANONICAL_BUILD()
AC_CANONICAL_HOST()
AC_CANONICAL_TARGET()

AM_INIT_AUTOMAKE([subdir-objects -Wall -Werror foreign])
AM_SILENT_RULES([yes])
AC_CONFIG_HEADERS([config.h])

AC_USE_SYSTEM_EXTENSIONS

LT_INIT

# Checks for programs.
AC_LANG([C])
AC_PROG_CC

AC_PROG_INSTALL
AC_PROG_LN_S

# TODO: eventually enable this
# AX_CHECK_COMPILE_FLAG([-Weverything], AM_CFLAGS+=" -Weverything")

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
AM_COND_IF(DEBUG,
  AC_DEFINE(DEBUG, 1, [Define to 0 if this is a release build]),
  AC_DEFINE(DEBUG, 0, [Define to 1 or higher if this is a debug build]))

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
  [AC_DEFINE(COVERAGE, 1, [Define to 0 if this is a release build])
   # Remove all optimization flags from CFLAGS
   changequote({,})
   CFLAGS=`echo "$CFLAGS" | $SED -e 's/-O[0-9]*//g'`
   changequote([,])],
  AC_DEFINE(COVERAGE, 0, [Define to 1 or higher if this is a test build]))

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

AC_ARG_ENABLE(experimental,
  AS_HELP_STRING(
    [--enable-experimental],
    [enable experimental features, default: no]),
  [case "${enableval}" in
     yes) experimental=true ;;
     no)  experimental=false ;;
     *)   AC_MSG_ERROR([bad value ${enableval} for --enable-experimental]) ;;
   esac],
  [experimental=false])
AM_CONDITIONAL(EXPERIMENTAL, test x"$experimental" = x"true")
AM_COND_IF(EXPERIMENTAL,
  AC_DEFINE(EXPERIMENTAL, 1, [Define to 0 to exclude experimental features]),
  AC_DEFINE(EXPERIMENTAL, 0, [Define to 1 to include experimental features]))

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
