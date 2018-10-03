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
