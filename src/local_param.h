#ifndef LOCAL_PARAM
#define LOCAL_PARAM

#include <Rinternals.h>

SEXP local_server(SEXP path, SEXP mode, SEXP timeout, SEXP backlog);
SEXP local_server_selectfd(SEXP con, SEXP mode);
SEXP local_server_accept(SEXP con);
SEXP local_server_activefds(SEXP con);
SEXP local_server_set_activefd(SEXP con, SEXP fd);

SEXP local_client(SEXP path, SEXP mode, SEXP timeout);

#endif
