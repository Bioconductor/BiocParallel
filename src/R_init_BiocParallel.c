#include <R_ext/Rdynload.h>
#include <Rinternals.h>
#include "ipcmutex.h"
#include "local_param.h"

static const R_CallMethodDef callMethods[] = {
    /* ipcmutex */
    /* uuid */
    {".ipc_uuid", (DL_FUNC) & ipc_uuid, 0},
    /* lock */
    {".ipc_lock", (DL_FUNC) & ipc_lock, 1},
    {".ipc_try_lock", (DL_FUNC) & ipc_try_lock, 1},
    {".ipc_unlock", (DL_FUNC) & ipc_unlock, 1},
    {".ipc_locked", (DL_FUNC) & ipc_locked, 1},
    /* counter */
    {".ipc_yield", (DL_FUNC) & ipc_yield, 1},
    {".ipc_value", (DL_FUNC) & ipc_value, 1},
    {".ipc_reset", (DL_FUNC) & ipc_reset, 2},
     /* cleanup */
    {".ipc_remove", (DL_FUNC) & ipc_remove, 1},

    /* local_param */
    {".local_server", (DL_FUNC) &local_server, 4},
    {".local_server_selectfd", (DL_FUNC) &local_server_selectfd, 2},
    {".local_server_accept", (DL_FUNC) &local_server_accept, 1},
    {".local_server_activefds", (DL_FUNC) &local_server_activefds, 1},
    {".local_server_set_activefd", (DL_FUNC) &local_server_set_activefd, 2},

    {".local_client", (DL_FUNC) &local_client, 3},

    {NULL, NULL, 0}
};

void R_init_BiocParallel(DllInfo *info)
{
    R_registerRoutines(info, NULL, callMethods, NULL, NULL);
    R_useDynamicSymbols(info, FALSE);
}

void R_unload_BiocParallel(DllInfo *info)
{
    (void) info;
}
