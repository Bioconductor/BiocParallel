## There are three timeouts involved
##
## - establishing a socket connection, from getOption("timeout"),
##   default 60 seconds
## - duration of allowed computations, from argument `timeout=` to
##   *Param(), default WORKER_TIMEOUT (infinite)
## - duration of idle connections (no activity from the worker
##   socket), default IDLE_TIMEOUT (30 days) beause (a) this is the
##   snow behavior and (b) sockets appear to sometimes segfault & lead
##   to PROTECTion imbalance if an attempt is made to write to a
##   terminated socket.

## Timeout for individual worker tasks
WORKER_TIMEOUT <- NA_integer_

## Timeout for socket inactivity
IDLE_TIMEOUT <- 2592000L # 60 * 60 * 24 * 30 = 30 day; consistent w/ parallel

## Maximum number of tasks, e.g., when using progress bar
TASKS_MAXIMUM <- .Machine$integer.max

.prototype_update <-
    function(prototype, ...)
{
    args <- list(...)
    stopifnot(
        all(names(args) %in% names(prototype))
    )
    prototype[names(args)] <- unname(args)
    prototype
}
