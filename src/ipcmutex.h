#ifndef IPCMUTEX
#define IPCMUTEX

#include <Rinternals.h>

#ifdef __cplusplus
extern "C" {
#endif    

SEXP ipc_uuid();

SEXP ipc_lock(SEXP id_sexp);
SEXP ipc_try_lock(SEXP id_sexp);
SEXP ipc_unlock(SEXP id_sexp);
SEXP ipc_locked(SEXP id_sexp);

SEXP ipc_yield(SEXP id_sexp);
SEXP ipc_value(SEXP id_sexp);
SEXP ipc_reset(SEXP id_sexp, SEXP n_sexp);

SEXP ipc_remove(SEXP id_sexp);


#ifdef __cplusplus
}
#endif    

#endif
