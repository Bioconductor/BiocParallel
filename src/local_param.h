#ifndef LOCAL_PARAM
#define LOCAL_PARAM

#include <Rinternals.h>

SEXP local_manager(SEXP path, SEXP mode, SEXP timeout, SEXP backlog);
SEXP local_manager_selectfd(SEXP con, SEXP mode);
SEXP local_manager_accept(SEXP con);
SEXP local_manager_activefds(SEXP con);
SEXP local_manager_set_activefd(SEXP con, SEXP fd);

SEXP local_worker(SEXP path, SEXP mode, SEXP timeout);

#endif
