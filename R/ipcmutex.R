## Utilities

ipcid <- function(id) {
    uuid <- .Call(.ipc_uuid)
    if (!missing(id))
        uuid <- paste(as.character(id), uuid, sep="-")
    uuid
}

ipcremove <- function(id) {
    invisible(.Call(.ipc_remove, id))
}

## Locks

ipclocked <- function(id)
    .Call(.ipc_locked, id)

ipclock <- function(id) {
    .Call(.ipc_lock, id)
}

ipctrylock <- function(id) {
    .Call(.ipc_try_lock, id)
}

ipcunlock <- function(id) {
    .Call(.ipc_unlock, id)
}

## Counters

ipcyield <- function(id) {
    .Call(.ipc_yield, id)
}

ipcvalue <- function(id) {
    .Call(.ipc_value, id)
}

ipcreset <- function(id, n = 1) {
    invisible(.Call(.ipc_reset, id, n))
}
