## Timeout for individual worker tasks
WORKER_TIMEOUT <- NA_integer_

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
