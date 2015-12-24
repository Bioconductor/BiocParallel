.log_load <- function(BPPARAM)
{
    if (!bplog(BPPARAM))
        return(invisible(NULL))
    if (!isNamespaceLoaded("futile.logger"))
        tryCatch({
            loadNamespace("futile.logger")
        }, error=function(err) {
            msg <- "logging requires the 'futile.logger' package"
            stop(conditionMessage(err), msg) 
        })
    futile.logger::flog.threshold(bpthreshold(BPPARAM))
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
