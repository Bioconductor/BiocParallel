### =========================================================================
### BiocParallelParam objects
### -------------------------------------------------------------------------

.BiocParallelParam <- setRefClass("BiocParallelParam",
    contains="VIRTUAL",
    fields=list(
        workers="ANY",
        .controlled="logical",  ## move to SnowParam ?
        catch.errors="logical", ## BatchJobs 
        stopOnError="logical",
        log="logical",
        threshold="ANY",
        logdir="character",
        resultdir="character"),
    methods=list(
      initialize = function(..., 
          workers=0, 
          .controlled=TRUE,
          catch.errors=TRUE,  ## BatchJobs
          stopOnError=FALSE,
          log=FALSE,
          threshold="INFO",
          logdir=character(),
          resultdir=character())
      {
          initFields(workers=workers, .controlled=.controlled,
                     catch.errors=catch.errors, stopOnError=stopOnError,
                     log=log, threshold=.THRESHOLD(threshold), logdir=logdir,
                     resultdir=resultdir)
          callSuper(...)
      },
      show = function() {
          cat("class:", class(.self), "\n")
          cat("bpisup:", bpisup(.self), "\n")
          cat("bpworkers:", bpworkers(.self), "\n")
          cat("bplog:", bplog(.self), "\n")
          cat("bpthreshold:", names(bpthreshold(.self)), "\n")
          cat("bplogdir:", bplogdir(.self), "\n")
          cat("bpresultdir:", bpresultdir(.self), "\n")
          cat("bpstopOnError:", bpstopOnError(.self), "\n")
      })
)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Validity
###


.valid.BiocParallelParam.log <- function(object) {
    msg <- NULL

    if (!.isTRUEorFALSE(bplog(object)))
        msg <- c(msg, "'bplog(BPPARAM)' must be logical(1)")

    threshold <- bpthreshold(object)
    if (length(threshold) != 1L) 
        return(c(msg, "'bpthreshold(BPPARAM)' must be character(1)"))

    nms <- c("TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL")
    if (!names(threshold) %in% nms) {
        return(c(msg, paste0("'bpthreshold(BPPARAM)' must be one of ", 
               paste(sQuote(nms), collapse=", "))))
    }

    dir <- bplogdir(object) 
    if (length(dir) > 1L || !is(dir, "character"))
        msg <- c(msg, "'bplogdir(BPPARAM)' must be character(1)")

    msg
}

.valid.BiocParallelParam.error <- function(object) {
    msg <- NULL
    ## FIXME: mutually exclusive with write to file?
    if (!.isTRUEorFALSE(bpstopOnError(object)))
        msg <- c(msg, "'bpstopOnError(BPPARAM)' must be logical(1)")
    if (bpstopOnError(object) && length(bpresultdir(object)))
        msg <- c(msg, paste0("when 'resultdir' is specified, 'stopOnError ",
                 "must be FALSE"))

    msg
}

.valid.BiocParallelParam.result <- function(object) {
    msg <- NULL

    dir <- bpresultdir(object) 
    if (length(dir) > 1L || !is(dir, "character"))
        msg <- c(msg, "'bpresultdir(BPPARAM)' must be character(1)")

    msg
}

setValidity("BiocParallelParam", function(object)
{
    msg <- NULL
    if (is.numeric(bpworkers(object))) 
        if (length(bpworkers(object)) != 1L || bpworkers(object) < 0)
            msg <- c(msg, "'workers' must be integer(1) and >= 0")
    if (is.character(bpworkers(object)))
        if (length(bpworkers(object)) < 1L)
            msg <- c(msg, "length(bpworkers(BPPARAM)) must be > 0") 
    if (!.isTRUEorFALSE(.controlled(object)))
        msg <- c(msg, "'.controlled' must be TRUE or FALSE")
    if (!.isTRUEorFALSE(object$catch.errors))
        msg <- c(msg, "'catch.errors' must be TRUE or FALSE")
    msg <- c(msg, 
             .valid.BiocParallelParam.log(object),
             .valid.BiocParallelParam.error(object),
             .valid.BiocParallelParam.result(object))
    if (is.null(msg)) TRUE else msg
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Getters / Setters
###

.controlled <-
    function(x)
{
    x$.controlled
}

setMethod(bpworkers, "BiocParallelParam",
   function(x, ...)
{
    x$workers
})

setMethod("bpstopOnError", "BiocParallelParam",
    function(x, ...)
{
    x$stopOnError
})

setReplaceMethod("bpstopOnError", c("BiocParallelParam", "logical"),
    function(x, ..., value)
{
    x$stopOnError <- value 
    x 
})

setMethod("bplog", "BiocParallelParam",
    function(x, ...)
{
    x$log
})

setReplaceMethod("bplog", c("BiocParallelParam", "logical"),
    function(x, ..., value)
{
    x$log <- value 
    x
})

setMethod("bpthreshold", "BiocParallelParam",
    function(x, ...)
{
    x$threshold
})

setReplaceMethod("bpthreshold", c("BiocParallelParam", "character"),
    function(x, ..., value)
{
    nms <- c("TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL")
    if (!value %in% nms)
        stop(paste0("'value' must be one of ",
             paste(sQuote(nms), collapse=", ")))
    x$threshold <- .THRESHOLD(value) 
    x
})

setMethod("bplogdir", "BiocParallelParam",
    function(x, ...)
{
    x$logdir
})

setReplaceMethod("bplogdir", c("BiocParallelParam", "character"),
    function(x, ..., value)
{
    x$logdir <- value 
    if (is.null(msg <- .valid.BiocParallelParam.log(x))) 
        x
    else 
        stop(msg)
})

setMethod("bpresultdir", "BiocParallelParam",
    function(x, ...)
{
    x$resultdir
})

setReplaceMethod("bpresultdir", c("BiocParallelParam", "character"),
    function(x, ..., value)
{
    x$resultdir <- value 
    if (is.null(msg <- .valid.BiocParallelParam.result(x))) 
        x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Helpers
###

.THRESHOLD <- function(xx) {
    if (!is.null(names(xx)))
        xx <- names(xx)
    switch(xx,
           "FATAL"=futile.logger::FATAL,
           "ERROR"=futile.logger::ERROR,
           "WARN"=futile.logger::WARN,
           "INFO"=futile.logger::INFO,
           "DEBUG"=futile.logger::DEBUG,
           "TRACE"=futile.logger::TRACE,
           xx)
}

## taken from S4Vectors
.isTRUEorFALSE <- function (x) {
    is.logical(x) && length(x) == 1L && !is.na(x)
}

bpok <- function(x) {
    if (!is(x, "list"))
        stop("'x' must be a list")

    lapply(x, function(elt) !is(elt, "try-error"))
}
