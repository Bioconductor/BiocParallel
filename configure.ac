AC_INIT([BiocParallel], 1.17.11) dnl package name, version

dnl check librt for shm_open support
dnl R usually is linked to librt but not always
AC_CHECK_LIB(rt, shm_open)
AC_SUBST(LIBS)
AC_CONFIG_FILES([src/Makevars])
AC_OUTPUT
