AC_INIT([BiocParallel], 1.32.4) dnl package name, version

CXX=`"${R_HOME}/bin/R" CMD config CXX`
if test -z "$CXX"; then
  AC_MSG_ERROR([No C++ compiler is available])
fi
CXXFLAGS=`"${R_HOME}/bin/R" CMD config CXXFLAGS`
CPPFLAGS=`"${R_HOME}/bin/R" CMD config CPPFLAGS`
AC_LANG(C++)

dnl check librt for shm_open support
dnl R usually is linked to librt but not always
AC_SEARCH_LIBS([shm_open], [rt])
AC_CHECK_HEADER(
        [sys/mman.h], [],
        AC_MSG_ERROR([cannot find required header sys/mman.h]))
AC_SUBST(LIBS)
AC_CONFIG_FILES([src/Makevars])
AC_OUTPUT
