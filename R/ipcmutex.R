## Utilities

ipcid <- function(id) {
    uuid <- cpp_ipc_uuid()
    if (!missing(id))
        uuid <- paste(as.character(id), uuid, sep="-")
    uuid
}

ipcremove <- function(id) {
    invisible(cpp_ipc_remove(id))
}

## Locks

ipclocked <- function(id)
    cpp_ipc_locked(id)

ipclock <- function(id) {
    cpp_ipc_lock(id)
}

ipctrylock <- function(id) {
    cpp_ipc_try_lock(id)
}

ipcunlock <- function(id) {
    cpp_ipc_unlock(id)
}

## Counters

ipcyield <- function(id) {
    cpp_ipc_yield(id)
}

ipcvalue <- function(id) {
    cpp_ipc_value(id)
}

ipcreset <- function(id, n = 1) {
    invisible(cpp_ipc_reset(id, n))
}
