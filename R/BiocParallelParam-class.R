### =========================================================================
### BiocParallelParam objects
### -------------------------------------------------------------------------

.BiocParallelParam <- setRefClass("BiocParallelParam",
    contains="VIRTUAL",
    fields=list(
        workers="ANY",
        tasks="integer",
        jobname="character",
        catch.errors="logical",  ## deprecated
        stop.on.error="logical",
        progressbar="logical"),
    methods=list(
        initialize = function(..., 
            workers=0, 
            tasks=0L, 
            jobname="BPJOB",
            catch.errors=TRUE,    ## deprecated
            stop.on.error=FALSE,
            progressbar=FALSE)
        {
            initFields(workers=workers, tasks=tasks, jobname=jobname, 
                       catch.errors=catch.errors, stop.on.error=stop.on.error, 
                       progressbar=progressbar)
            callSuper(...)
        },
        show = function() {
            cat("class:", class(.self), "\n")
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

    ## error handling
    if (!.isTRUEorFALSE(bpcatchErrors(object)))
        msg <- c(msg, "'catch.errors' must be TRUE or FALSE")
    if (!.isTRUEorFALSE(bpstopOnError(object)))
        msg <- c(msg, "'bpstopOnError(BPPARAM)' must be logical(1)")

    if (is.null(msg)) TRUE else msg
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Getters / Setters
###

setMethod(bpworkers, "BiocParallelParam",
   function(x, ...)
{
    x$workers
})

setMethod(bptasks, "BiocParallelParam",
   function(x, ...)
{
    x$tasks
})

setReplaceMethod("bptasks", c("BiocParallelParam", "numeric"),
    function(x, ..., value)
{
    x$tasks <- as.integer(value)
    x 
})

setMethod(bpjobname, "BiocParallelParam",
   function(x, ...)
{
    x$jobname
})

setReplaceMethod("bpjobname", c("BiocParallelParam", "character"),
    function(x, ..., value)
{
    x$jobname <- value
    x 
})


setMethod("bpstopOnError", "BiocParallelParam",
    function(x, ...)
{
    x$stop.on.error
})

setReplaceMethod("bpstopOnError", c("BiocParallelParam", "logical"),
    function(x, ..., value)
{
    x$stop.on.error <- value 
    validObject(x)
    x 
})

setMethod("bpcatchErrors", "BiocParallelParam",
    function(x, ...)
{
    x$catch.errors
})

setReplaceMethod("bpcatchErrors", c("BiocParallelParam", "logical"),
    function(x, ..., value)
{
    x$catch.errors <- value 
    x 
})

setMethod("bpprogressbar", "BiocParallelParam",
    function(x, ...)
{
    x$progressbar
})

setReplaceMethod("bpprogressbar", c("BiocParallelParam", "logical"),
    function(x, ..., value)
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

bpok <- function(x) {
    if (is.null(x))
        x
    else if (!is.list(x))
        stop("'x' must be a list")
    else
        sapply(x, function(elt) !is(elt, "condition"))
}



