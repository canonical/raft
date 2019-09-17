AC_PREREQ(2.60)
AC_INIT([raft], [0.9.6])
AC_LANG([C])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_AUX_DIR([ac])

AM_INIT_AUTOMAKE([subdir-objects -Wall -Wno-portability foreign])
AM_SILENT_RULES([yes])
AC_USE_SYSTEM_EXTENSIONS # Defines _GNU_SOURCE and similar

LT_INIT

# The libuv raft_io implementation is built by default if libuv is found, unless
# explicitely disabled.
AC_ARG_ENABLE(uv, AS_HELP_STRING([--disable-uv], [do not build the libuv-based raft_io implementation]))
AS_IF([test "x$enable_uv" != "xno"],
      [PKG_CHECK_MODULES(UV, [libuv >= 1.8.0], [have_uv=yes], [have_uv=no])],
      [have_uv=no])
AS_IF([test "x$enable_uv" = "xyes" -a "x$have_uv" = "xno"], [AC_MSG_ERROR([libuv required but not found])], [])
AM_CONDITIONAL(UV_ENABLED, test "x$have_uv" = "xyes")

# The fake I/O implementation and associated fixture is built by default, unless
# explicitely disabled.
AC_ARG_ENABLE(fixture, AS_HELP_STRING([--disable-fixture], [do not build the raft_fixture test helper]))
AM_CONDITIONAL(FIXTURE_ENABLED, test "x$enable_fixture" != "xno")

# The example program is optional.
AC_ARG_ENABLE(example, AS_HELP_STRING([--enable-example[=ARG]], [build the example program [default=no]]))
AS_IF([test "x$enable_example" = "xyes" -a "x$have_uv" = "xno"], [AC_MSG_ERROR([example program requires libuv])], [])
AM_CONDITIONAL(EXAMPLE_ENABLED, test "x$enable_example" = "xyes")

# Whether to enable debugging code.
AC_ARG_ENABLE(debug, AS_HELP_STRING([--enable-debug[=ARG]], [enable debugging [default=no]]))
AM_CONDITIONAL(DEBUG_ENABLED, test "x$enable_debug" = "xyes")

# Whether to enable memory sanitizer.
AC_ARG_ENABLE(sanitize, AS_HELP_STRING([--enable-sanitize[=ARG]], [enable code sanitizers [default=no]]))
AM_CONDITIONAL(SANITIZE_ENABLED, test x"$enable_sanitize" = x"yes")

# Whether to enable code coverage.
AX_CODE_COVERAGE

# Checks for header files.
AC_CHECK_HEADERS([stdlib.h string.h stdio.h assert.h unistd.h])

# Check if btrfs is available (for enabling btrfs-related tests).
AC_CHECK_PROG(have_btrfs, btrfs, yes)
AS_IF([test x"$have_btrfs" = x"yes"], [AC_DEFINE(RAFT_HAVE_BTRFS)], [])

# Check if xfs is available (for enabling xfs-related tests).
AC_CHECK_PROG(have_xfs, xfs, yes)
AS_IF([test x"$have_xfs" = x"yes"], [AC_DEFINE(RAFT_HAVE_XFS)], [])

# Check if zfs is available (for enabling zfs-related tests).
AC_CHECK_PROG(have_zfs, zfs, yes)
AS_IF([test x"$have_zfs" = x"yes"],
   [AC_DEFINE(RAFT_HAVE_ZFS)
    AX_COMPARE_VERSION($(zfs version | cut -f 2 -d - | head -1), [ge], [0.8.0],
       [AC_DEFINE(RAFT_HAVE_ZFS_WITH_DIRECT_IO)], [])
   ],
   [])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_CHECK_FUNCS([memcpy vsprintf])

AX_CHECK_COMPILE_FLAG([-std=c99], [AM_CFLAGS="$AM_CFLAGS -std=c99"])
AX_CHECK_COMPILE_FLAG([-fvisibility=hidden], [AM_CFLAGS="$AM_CFLAGS -fvisibility=hidden"])
AX_CHECK_COMPILE_FLAG([-fdiagnostics-color], [AM_CFLAGS="$AM_CFLAGS -fdiagnostics-color"])
AX_CHECK_COMPILE_FLAG([-Wpedantic], [AM_CFLAGS="$AM_CFLAGS -Wpedantic"])
AX_CHECK_COMPILE_FLAG([-Wall], [AM_CFLAGS="$AM_CFLAGS -Wall"])
AX_CHECK_COMPILE_FLAG([-Wimplicit-fallthrough=5], [AM_CFLAGS="$AM_CFLAGS -Wimplicit-fallthrough=5"])
AX_CHECK_COMPILE_FLAG([-Wcast-align], [AM_CFLAGS="$AM_CFLAGS -Wcast-align"])
AX_CHECK_COMPILE_FLAG([-Wstrict-prototypes], [AM_CFLAGS="$AM_CFLAGS -Wstrict-prototypes"])
AX_CHECK_COMPILE_FLAG([-fno-strict-aliasing], [AM_CFLAGS="$AM_CFLAGS -fno-strict-aliasing"])
AX_CHECK_COMPILE_FLAG([-fstack-clash-protection], [AM_CFLAGS="$AM_CFLAGS -fstack-clash-protection"])
AX_CHECK_COMPILE_FLAG([-fstack-protector-strong], [AM_CFLAGS="$AM_CFLAGS -fstack-protector-strong"])
AX_CHECK_COMPILE_FLAG([--param=ssp-buffer-size=4], [AM_CFLAGS="$AM_CFLAGS --param=ssp-buffer-size=4"])
AX_CHECK_COMPILE_FLAG([-g], [AM_CFLAGS="$AM_CFLAGS -g"])
AX_CHECK_COMPILE_FLAG([--mcet -fcf-protection], [AM_CFLAGS="$AM_CFLAGS --mcet -fcf-protection"])
AX_CHECK_COMPILE_FLAG([-Werror=implicit-function-declaration], [AM_CFLAGS="$AM_CFLAGS -Werror=implicit-function-declaration"])
AX_CHECK_COMPILE_FLAG([-Wlogical-op], [AM_CFLAGS="$AM_CFLAGS -Wlogical-op"])
AX_CHECK_COMPILE_FLAG([-Wmissing-include-dirs], [AM_CFLAGS="$AM_CFLAGS -Wmissing-include-dirs"])
AX_CHECK_COMPILE_FLAG([-Wold-style-definition], [AM_CFLAGS="$AM_CFLAGS -Wold-style-definition"])
AX_CHECK_COMPILE_FLAG([-Winit-self], [AM_CFLAGS="$AM_CFLAGS -Winit-self"])
AX_CHECK_COMPILE_FLAG([-Wfloat-equal], [AM_CFLAGS="$AM_CFLAGS -Wfloat-equal"])
AX_CHECK_COMPILE_FLAG([-Wsuggest-attribute=noreturn], [AM_CFLAGS="$AM_CFLAGS -Wsuggest-attribute=noreturn"])
AX_CHECK_COMPILE_FLAG([-Werror=return-type], [AM_CFLAGS="$AM_CFLAGS -Werror=return-type"])
AX_CHECK_COMPILE_FLAG([-Werror=incompatible-pointer-types], [AM_CFLAGS="$AM_CFLAGS -Werror=incompatible-pointer-types"])
AX_CHECK_COMPILE_FLAG([-Wformat=2], [AM_CFLAGS="$AM_CFLAGS -Wformat=2"])
AX_CHECK_COMPILE_FLAG([-Wshadow], [AM_CFLAGS="$AM_CFLAGS -Wshadow"])
AX_CHECK_COMPILE_FLAG([-Wendif-labels], [AM_CFLAGS="$AM_CFLAGS -Wendif-labels"])
AX_CHECK_COMPILE_FLAG([-Werror=overflow], [AM_CFLAGS="$AM_CFLAGS -Werror=overflow"])
AX_CHECK_COMPILE_FLAG([-fdiagnostics-show-option], [AM_CFLAGS="$AM_CFLAGS -fdiagnostics-show-option"])
AX_CHECK_COMPILE_FLAG([-Werror=shift-count-overflow], [AM_CFLAGS="$AM_CFLAGS -Werror=shift-count-overflow"])
AX_CHECK_COMPILE_FLAG([-Werror=shift-overflow=2], [AM_CFLAGS="$AM_CFLAGS -Werror=shift-overflow=2"])
AX_CHECK_COMPILE_FLAG([-Wdate-time], [AM_CFLAGS="$AM_CFLAGS -Wdate-time"])
AX_CHECK_COMPILE_FLAG([-Wnested-externs], [AM_CFLAGS="$AM_CFLAGS -Wnested-externs"])
AX_CHECK_COMPILE_FLAG([-fasynchronous-unwind-tables], [AM_CFLAGS="$AM_CFLAGS -fasynchronous-unwind-tables"])
AX_CHECK_COMPILE_FLAG([-pipe], [AM_CFLAGS="$AM_CFLAGS -pipe"])
AX_CHECK_COMPILE_FLAG([-fexceptions], [AM_CFLAGS="$AM_CFLAGS -fexceptions"])
AC_SUBST(AM_CFLAGS)

AC_CONFIG_FILES([raft.pc Makefile])
AC_OUTPUT
