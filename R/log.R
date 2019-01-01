.log_load <- function(log, threshold, appender = FALSE)
{
    if (!log)
        return(invisible(NULL))
    if (!isNamespaceLoaded("futile.logger"))
        tryCatch({
            loadNamespace("futile.logger")
        }, error=function(err) {
            msg <- "logging requires the 'futile.logger' package"
            stop(conditionMessage(err), msg) 
        })
    if (appender)
        futile.logger::flog.appender(.log_buffer_append, 'ROOT')
    futile.logger::flog.threshold(threshold)
    futile.logger::flog.info("loading futile.logger package")
}

.log_warn <- function(log, fmt, ...)
{
    if (log)
        futile.logger::flog.warn(fmt, ...)
}

.log_error <- function(log, fmt, ...)
{
    if (log)
        futile.logger::flog.error(fmt, ...)
}

## logging buffer

.log_buffer <- local({
    env <- new.env(parent=emptyenv())
    env[["buffer"]] <- character()
    env
})

.log_buffer_init <- function()
{
    .log_buffer[["buffer"]] <- NULL
}

.log_buffer_append <- function(line)
{
    .log_buffer[["buffer"]] <- c(.log_buffer[["buffer"]], line)
}

.log_buffer_get <- function()
{
    .log_buffer[["buffer"]]
}
