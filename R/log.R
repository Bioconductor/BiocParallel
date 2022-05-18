.log_data <- local({
    env <- new.env(parent=emptyenv())
    env[["buffer"]] <- character()
    env
})

.log_load <- function(log, threshold)
{
    if (!log) {
        if (isNamespaceLoaded("futile.logger")) {
            futile.logger::flog.appender(
                futile.logger::appender.console(),
                'ROOT'
                )
        }
        return(invisible(NULL))
    }

    ## log == TRUE
    if (!isNamespaceLoaded("futile.logger"))
        tryCatch({
            loadNamespace("futile.logger")
        }, error=function(err) {
            msg <- "logging requires the 'futile.logger' package"
            stop(conditionMessage(err), msg)
        })
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

.log_buffer_init <- function()
{
    .log_data[["buffer"]] <- character()
}

.log_buffer_append <- function(line)
{
    .log_data[["buffer"]] <- c(.log_data[["buffer"]], line)
}

.log_buffer_get <- function()
{
    .log_data[["buffer"]]
}


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### logs and results printed in the manager process
###

.bpwriteLog <- function(con, d) {
    .log_internal <- function() {
        message(
            "############### LOG OUTPUT ###############\n",
            "Task: ", d$value$tag,
            "\nNode: ", d$node,
            "\nTimestamp: ", Sys.time(),
            "\nSuccess: ", d$value$success,
            "\n\nTask duration:\n",
            paste(capture.output(d$value$time), collapse="\n"),
            "\n\nMemory used:\n", paste(capture.output(gc()), collapse="\n"),
            "\n\nLog messages:\n",
            paste(trimws(d$value$log), collapse="\n"),
            "\n\nstderr and stdout:\n",
            if (!is.null(d$value$sout))
                paste(noquote(d$value$sout), collapse="\n")
        )
    }
    if (!is.null(con)) {
        on.exit({
            sink(NULL, type = "message")
            sink(NULL, type = "output")
        })
        sink(con, type = "message")
        sink(con, type = "output")
    }
    .log_internal()
}
