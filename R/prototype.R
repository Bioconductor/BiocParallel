## Timeout for individual worker tasks
WORKER_TIMEOUT <- NA_integer_

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
