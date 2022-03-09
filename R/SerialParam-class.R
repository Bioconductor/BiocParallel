### =========================================================================
### SerialParam objects
### -------------------------------------------------------------------------

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

.SerialParam_prototype <- c(
    list(
        workers = 1L,
        backend = NULL
    ),
    .BiocParallelParam_prototype
)

.SerialParam <- setRefClass(
    "SerialParam",
    fields=list(backend = "ANY"),
    contains="BiocParallelParam",
)

SerialParam <-
    function(stop.on.error = TRUE,
             progressbar=FALSE,
             RNGseed = NULL,
             timeout = WORKER_TIMEOUT,
             log=FALSE, threshold="INFO", logdir=NA_character_,
             resultdir = NA_character_,
             jobname = "BPJOB",
             force.GC = FALSE)
{
    if (!is.null(RNGseed))
        RNGseed <- as.integer(RNGseed)

    if (progressbar) {
        tasks <- TASKS_MAXIMUM
    } else {
        tasks <- 0L
    }

    prototype <- .prototype_update(
        .SerialParam_prototype,
        tasks = tasks,
        stop.on.error=stop.on.error,
        progressbar=progressbar,
        RNGseed = RNGseed,
        timeout = as.integer(timeout),
        log=log,
        threshold=threshold,
        logdir=logdir,
        resultdir = resultdir,
        jobname = jobname,
        force.GC = force.GC,
        fallback = FALSE,
        exportglobals = FALSE,
        exportvariables = FALSE
    )
    x <- do.call(.SerialParam, prototype)
    validObject(x)
    x
}

setAs("BiocParallelParam", "SerialParam", function(from) {
    SerialParam(
        stop.on.error = bpstopOnError(from),
        progressbar = bpprogressbar(from),
        RNGseed = bpRNGseed(from),
        timeout = bptimeout(from),
        log = bplog(from),
        threshold = bpthreshold(from),
        logdir = bplogdir(from),
        resultdir = bpresultdir(from),
        jobname = bpjobname(from),
        force.GC = bpforceGC(from)
    )
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - control
###

setMethod(
    "bpbackend", "SerialParam",
    function(x)
{
    x$backend
})

setMethod(
    "bpstart", "SerialParam",
    function(x, ...)
{
    x$backend <- .SerialBackend()
    x$backend$BPPARAM <- x
    .bpstart_impl(x)
})

setMethod(
    "bpstop", "SerialParam",
    function(x)
{
    x$backend <- NULL
    .bpstop_impl(x)
})

setMethod(
    "bpisup", "SerialParam",
    function(x)
{
    is.environment(bpbackend(x))
})

setReplaceMethod("bplog", c("SerialParam", "logical"),
    function(x, value)
{
    x$log <- value
    validObject(x)
    x
})

setReplaceMethod(
    "bpthreshold", c("SerialParam", "character"),
    function(x, value)
{
    x$threshold <- value
    x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Backend method
###
.SerialBackend <- setClass("SerialBackend", contains = "environment")

setMethod(".send_to", "SerialBackend",
          function(backend, node, value){
    backend$value <- value
    TRUE
})

setMethod(
    ".recv_any", "SerialBackend",
    function(backend)
{
    on.exit(backend$value <- NULL)
    msg <- backend$value
    if (inherits(msg, "error"))
        stop(msg)
    if (msg$type == "EXEC") {
        value <- .bpworker_EXEC(msg, bplog(backend$BPPARAM))
        list(node = 1L, value = value)
    }
})

setMethod("length", "SerialBackend",
          function(x){
              1L
})
