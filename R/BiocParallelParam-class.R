### =========================================================================
### BiocParallelParam objects
### -------------------------------------------------------------------------

.BiocParallelParam_prototype <- list(
    workers=0,
    tasks=0L,
    jobname="BPJOB",
    log=FALSE,
    logdir = NA_character_,
    threshold="INFO",
    resultdir = NA_character_,
    stop.on.error=TRUE,
    timeout=.Machine$integer.max,
    exportglobals=TRUE,
    progressbar=FALSE,
    RNGseed=NULL,
    RNGstream = NULL,
    force.GC = FALSE
)

.BiocParallelParam <- setRefClass("BiocParallelParam",
    contains="VIRTUAL",
    fields=list(
        workers="ANY",
        tasks="integer",
        jobname="character",
        progressbar="logical",
        ## required for composeTry
        log="logical",
        logdir = "character",
        threshold="character",
        resultdir = "character",
        stop.on.error="logical",
        timeout="integer",
        exportglobals="logical",
        RNGseed = "ANY",        # NULL or integer(1)
        RNGstream = "ANY",      # NULL or integer(); internal use only
        force.GC = "logical",
        ## cluster management
        .finalizer_env = "environment",
        .uid = "character"
    ),
    methods=list(
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
                "  bpRNGseed: ", bpRNGseed(.self),
                "; bptimeout: ", bptimeout(.self),
                "; bpprogressbar: ", bpprogressbar(.self),
                "\n",
                "  bpexportglobals: ", bpexportglobals(.self),
                "; bpforceGC: ", bpforceGC(.self),
                "\n", .prettyPath("  bplogdir", bplogdir(.self)),
                "\n", .prettyPath("  bpresultdir", bpresultdir(.self)),
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
        msg <- c(msg, "length(bptasks(BPPARAM)) must be == 1")

    if (is.character(workers)) {
        if (length(workers) < 1L)
            msg <- c(msg, "length(bpworkers(BPPARAM)) must be > 0") 
        if (tasks > 0L && tasks < workers)
            msg <- c(msg, "number of tasks is less than number of workers")
    }

    if (!.isTRUEorFALSE(bpexportglobals(object)))
        msg <- c(msg, "'bpexportglobals' must be TRUE or FALSE")

    if (!.isTRUEorFALSE(bplog(object)))
        msg <- c(msg, "'bplog' must be logical(1)")

    ## log / logdir
    dir <- bplogdir(object)
    if (length(dir) != 1L || !is(dir, "character")) {
        msg <- c(msg, "'logdir' must be character(1)")
    } else if (!is.na(dir)) {
        if (!bplog(object))
            msg <- c(msg, "'log' must be TRUE when 'logdir' is given")
        if (!.dir_valid_rw(dir))
            msg <- c(msg, "'logdir' must exist with read / write permission")
    }

    ## resultdir
    dir <- bpresultdir(object)
    if (length(dir) != 1L || !is(dir, "character")) {
        msg <- c(msg, "'resultdir' must be character(1)")
    } else if (!is.na(dir) && !.dir_valid_rw(dir)) {
        msg <- c(msg, "'resultdir' must exist with read / write permissions")
    }

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
        msg <- c(msg, "'bpstopOnError' must be TRUE or FALSE")

    if (!.isTRUEorFALSE(bpforceGC(object)))
        msg <- c(msg, "'force.GC' must be TRUE or FALSE")

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

setMethod("bplogdir", "BiocParallelParam",
    function(x)
{
    x$logdir
})

setReplaceMethod("bplogdir", c("BiocParallelParam", "character"),
    function(x, value)
{
    if (bpisup(x))
        stop("use 'bpstop()' before setting 'bplogdir()'")

    x$logdir <- value
    validObject(x)
    x
})

setMethod("bpthreshold", "BiocParallelParam",
    function(x)
{
    x$threshold
})

setMethod("bpresultdir", "BiocParallelParam",
    function(x)
{
    x$resultdir
})

setReplaceMethod("bpresultdir", c("BiocParallelParam", "character"),
    function(x, value)
{
    if (bpisup(x))
        stop("use 'bpstop()' before setting 'bpresultdir()'")

    x$resultdir <- value
    validObject(x)
    x
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

setMethod("bpprogressbar", "BiocParallelParam",
    function(x)
{
    x$progressbar
})

setReplaceMethod("bpprogressbar", c("BiocParallelParam", "logical"),
    function(x, value)
{
    x$progressbar <- value
    validObject(x)
    x
})

setMethod("bpRNGseed", "BiocParallelParam",
    function(x)
{
    x$RNGseed
})

setReplaceMethod("bpRNGseed", c("BiocParallelParam", "NULL"),
    function(x, value)
{
    x$RNGseed <- NULL
    validObject(x)
    x
})

setReplaceMethod("bpRNGseed", c("BiocParallelParam", "numeric"),
    function(x, value)
{
    x$RNGseed <- as.integer(value)
    validObject(x)
    x
})

.RNGstream <-
    function(x)
{
    x$RNGstream
}

`.RNGstream<-` <-
    function(x, value)
{
    value <- as.integer(value)
    if (anyNA(value))
        stop("[internal] RNGstream value could not be coerced to integer")
    x$RNGstream <- value
    x
}

.bpnextRNGstream <-
    function(x)
{
    ## initialize or get the next random number stream; increment the
    ## stream only in bpstart_impl
    .RNGstream(x) <- .rng_next_stream(.RNGstream(x))
}

setMethod("bpforceGC", "BiocParallelParam",
    function(x)
{
    x$force.GC
})

setReplaceMethod("bpforceGC", c("BiocParallelParam", "numeric"),
    function(x, value)
{
    x$force.GC <- as.logical(value)
    validObject(x)
    x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - evaluation
###

setMethod("bpstart", "BiocParallelParam", .bpstart_impl)

setMethod("bpstop", "BiocParallelParam", .bpstop_impl)

setMethod("bplapply", c("ANY", "BiocParallelParam"), .bplapply_impl)

setMethod("bpiterate", c("ANY", "ANY", "BiocParallelParam"), .bpiterate_impl)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Helpers
###

## taken from S4Vectors
.isTRUEorFALSE <- function (x) {
    is.logical(x) && length(x) == 1L && !is.na(x)
}
