### =========================================================================
### BiocParallelParam objects
### -------------------------------------------------------------------------

.BiocParallelParam <- setRefClass("BiocParallelParam",
    contains="VIRTUAL",
    fields=list(
        workers="ANY",
        tasks="integer",
        jobname="character",
        progressbar="logical",
        ## required for composeTry
        log="logical",
        threshold="character",
        stop.on.error="logical",
        timeout="integer",
        exportglobals="logical",
        ## deprecated
        catch.errors="logical"
    ),
    methods=list(
        initialize = function(..., 
            workers=0, 
            tasks=0L, 
            jobname="BPJOB",
            catch.errors=TRUE,    ## deprecated
            log=FALSE,
            threshold="INFO",
            stop.on.error=TRUE,
            timeout=30L * 24L * 60L * 60L, # 30 days
            exportglobals=TRUE,
            progressbar=FALSE)
        {
            callSuper(...)
            initFields(workers=workers, tasks=tasks, jobname=jobname, 
                       catch.errors=catch.errors, log=log, threshold=threshold,
                       stop.on.error=stop.on.error, timeout=as.integer(timeout),
                       exportglobals=exportglobals, progressbar=progressbar)
        },
        show = function() {
            cat("class: ", class(.self),
                "\n",
                "  bpisup: ", bpisup(.self),
                "; bpnworkers: ", bpnworkers(.self),
                "; bptasks: ", bptasks(.self),
                "; bpjobname: ", bpjobname(.self),
                "\n",
                "  bplog: ", bplog(.self),
                "; bpthreshold: ", bpthreshold(.self),
                "; bpstopOnError: ", bpstopOnError(.self),
                "\n",
                "  bptimeout: ", bptimeout(.self),
                "; bpprogressbar: ", bpprogressbar(.self),
                "; bpexportglobals: ", bpexportglobals(.self),
                "\n", sep="")
        })
)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Validity
###

setValidity("BiocParallelParam", function(object)
{
    msg <- NULL

    ## workers and tasks
    workers <- bpworkers(object)
    if (is.numeric(workers)) 
        if (length(workers) != 1L || workers < 0)
            msg <- c(msg, "'workers' must be integer(1) and >= 0")

    tasks <- bptasks(object)
    if (!is.numeric(tasks))
        msg <- c(msg, "bptasks(BPPARAM) must be an integer")
    if (length(tasks) > 1L)
        msg <- c(msg, "length(bpwtasks(BPPARAM)) must be == 1") 

    if (is.character(workers)) {
        if (length(workers) < 1L)
            msg <- c(msg, "length(bpworkers(BPPARAM)) must be > 0") 
        if (tasks > 0L && tasks < workers)
            msg <- c(msg, "number of tasks is less than number of workers")
    }

    if (!.isTRUEorFALSE(bpexportglobals(object)))
        msg <- c(msg, "'bpexportglobals' must be TRUE or FALSE")

    ## error handling
    if (!.isTRUEorFALSE(bpcatchErrors(object)))
        msg <- c(msg, "'catch.errors' must be TRUE or FALSE")

    if (!.isTRUEorFALSE(bplog(object)))
        msg <- c(msg, "'bplog' must be logical(1)")

    levels <- c("TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL")
    threshold <- bpthreshold(object)
    if (length(threshold) > 1L) {
        msg <- c(msg, "'bpthreshold' must be character(0) or character(1)")
    } else if ((length(threshold) == 1L) && (!threshold %in% levels)) {
        txt <- sprintf("'bpthreshold' must be one of %s",
                       paste(sQuote(levels), collapse=", "))
        msg <- c(msg, paste(strwrap(txt, indent=2, exdent=2), collapse="\n"))
    }

    if (!.isTRUEorFALSE(bpstopOnError(object)))
        msg <- c(msg, "'bpstopOnError(BPPARAM)' must be logical(1)")

    if (is.null(msg)) TRUE else msg
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Getters / Setters
###

setMethod("bpworkers", "BiocParallelParam",
   function(x)
{
    x$workers
})

setMethod("bptasks", "BiocParallelParam",
   function(x)
{
    x$tasks
})

setReplaceMethod("bptasks", c("BiocParallelParam", "numeric"),
    function(x, value)
{
    x$tasks <- as.integer(value)
    x 
})

setMethod("bpjobname", "BiocParallelParam",
   function(x)
{
    x$jobname
})

setReplaceMethod("bpjobname", c("BiocParallelParam", "character"),
    function(x, value)
{
    x$jobname <- value
    x 
})

setMethod("bplog", "BiocParallelParam",
    function(x)
{
    x$log
})

setMethod("bpthreshold", "BiocParallelParam",
    function(x)
{
    x$threshold
})

setMethod("bptimeout", "BiocParallelParam",
    function(x)
{
    x$timeout
})

setReplaceMethod("bptimeout", c("BiocParallelParam", "numeric"),
    function(x, value)
{
    x$timeout <- as.integer(value)
    x
})

setMethod("bpexportglobals", "BiocParallelParam",
    function(x)
{
    x$exportglobals
})

setReplaceMethod("bpexportglobals", c("BiocParallelParam", "logical"),
    function(x, value)
{
    x$exportglobals <- value
    validObject(x)
    x
})

setMethod("bpstopOnError", "BiocParallelParam",
    function(x)
{
    x$stop.on.error
})

setReplaceMethod("bpstopOnError", c("BiocParallelParam", "logical"),
    function(x, value)
{
    x$stop.on.error <- value 
    validObject(x)
    x 
})

setMethod("bpcatchErrors", "BiocParallelParam",
    function(x)
{
    x$catch.errors
})

setReplaceMethod("bpcatchErrors", c("BiocParallelParam", "logical"),
    function(x, value)
{
    x$catch.errors <- value 
    x 
})

setMethod("bpprogressbar", "BiocParallelParam",
    function(x)
{
    x$progressbar
})

setReplaceMethod("bpprogressbar", c("BiocParallelParam", "logical"),
    function(x, value)
{
    x$progressbar <- value 
    x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Helpers
###

## taken from S4Vectors
.isTRUEorFALSE <- function (x) {
    is.logical(x) && length(x) == 1L && !is.na(x)
}
