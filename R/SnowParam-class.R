### =========================================================================
### SnowParam objects
### -------------------------------------------------------------------------

## 11 February, 2014 -- this is a hack: import'ing
## parallel::stopCluster and then library(snow) overwrites
## parallel::stopCluster.default with snow::stopCluster.default,
## resulting in incorrect dispatch to snow::sendData
stopCluster <- parallel::stopCluster
makeCluster <- parallel::makeCluster

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor 
###

snowWorkers <- function() {
    cores <- min(8L, parallel::detectCores())
    getOption("mc.cores", cores)
}

setOldClass(c("NULLcluster", "cluster"))

.NULLcluster <-
    function()
{
    cl <- list()
    class(cl) <- c("NULLcluster", "cluster")
    cl
}

.SnowParam <- setRefClass("SnowParam",
    contains="BiocParallelParam",
    fields=list(
        cluster="cluster",
        .clusterargs="list",
        .controlled="logical",
        log="logical",
        threshold="ANY",
        logdir="character",
        resultdir="character"),
    methods=list(
        initialize = function(..., 
            .controlled=TRUE,
            log=FALSE,
            threshold="INFO",
            logdir=character(),
            resultdir=character())
        { 
            initFields(.controlled=.controlled, log=log, threshold=threshold, 
                       logdir=logdir, resultdir=resultdir)
            callSuper(...)
        },
        show = function() {
            callSuper()
            cat("bpisup:", bpisup(.self), "\n")
            cat("bplog:", bplog(.self), "\n")
            cat("bpthreshold:", names(bpthreshold(.self)), "\n")
            cat("bplogdir:", bplogdir(.self), "\n")
            cat("bpresultdir:", bpresultdir(.self), "\n")
            cat("bpstopOnError:", bpstopOnError(.self), "\n")
            cat("cluster type: ", .clusterargs$type, "\n")
        })
)

SnowParam <- function(workers=snowWorkers(), type=c("SOCK", "MPI", "FORK"), 
                      stopOnError=FALSE, log=FALSE, threshold="INFO", 
                      logdir=character(), resultdir=character(), ...)
{
    type <- match.arg(type)
    if (!type %in% c("SOCK", "MPI", "FORK"))
        stop("type must be one of 'SOCK', 'MPI' or 'FORK'")
    if (any(type %in% c("MPI", "FORK")) && is(workers, "character"))
        stop("'workers' must be integer(1) when 'type' is MPI or FORK") 

    args <- c(list(spec=workers, type=type), list(...)) 
    # FIXME I don't think this is required, lists always inflict a copy
    .clusterargs <- lapply(args, force)
    cluster <- .NULLcluster()
 
    x <- .SnowParam(.clusterargs=.clusterargs, cluster=cluster, 
                    .controlled=TRUE, workers=workers, 
                    stopOnError=stopOnError, log=log, 
                    threshold=.THRESHOLD(threshold), logdir=logdir,
                    resultdir=resultdir, ...)
    validObject(x)
    x
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Validity
###

.valid.SnowParam.log <- function(object) {
    msg <- NULL

    if (!.isTRUEorFALSE(bplog(object)))
        msg <- c(msg, "'bplog(BPPARAM)' must be logical(1)")

    if (object$.controlled) {
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
    }

    msg
}

.valid.SnowParam.result <- function(object) {
    msg <- NULL

    dir <- bpresultdir(object) 
    if (length(dir) > 1L || !is(dir, "character"))
        msg <- c(msg, "'bpresultdir(BPPARAM)' must be character(1)")

    msg
}

setValidity("SnowParam", function(object)
{
    msg <- NULL
    if (!.isTRUEorFALSE(.controlled(object)))
        msg <- c(msg, "'.controlled' must be TRUE or FALSE")
    msg <- c(msg, 
             .valid.SnowParam.log(object),
             .valid.SnowParam.result(object))
    if (is.null(msg)) TRUE else msg
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - control
###

setMethod(bpworkers, "SnowParam",
    function(x, ...)
{
    if (bpisup(x))
        length(bpbackend(x))
    else
        x$workers
})

setMethod(bpstart, "SnowParam",
    function(x, tasks = bpworkers(x), ...)
{
    if (!.controlled(x))
        stop("'bpstart' not available; instance from outside BiocParallel?")
    if (bpisup(x))
        stop("cluster already started")
    if (!"package:parallel" %in% search()) {
        tryCatch({
            attachNamespace("parallel")
        }, error=function(err) {
            stop(conditionMessage(err), 
                "SnowParam class objects require the 'parallel' package")
        })
    }

    if (tasks > 0L)
        x$.clusterargs$spec <- min(bpworkers(x), tasks) 
    if (bplog(x)) {
        ## worker script in BiocParallel
        if (x$.clusterargs$type == "FORK") {
            bpbackend(x) <- do.call(.bpmakeForkCluster, 
                                    c(list(nnodes=x$.clusterargs$spec),
                                      x$.clusterargs))
        } else {
            x$.clusterargs$useRscript <- FALSE
            x$.clusterargs$scriptdir <- find.package("BiocParallel")
            bpbackend(x) <- do.call(makeCluster, x$.clusterargs)
        }
        .initiateLogging(x)
    } else {
        ## worker script in snow/parallel 
        x$.clusterargs$useRscript <- TRUE 
        bpbackend(x) <- do.call(makeCluster, x$.clusterargs)
    }
    invisible(x)
})

setMethod(bpstop, "SnowParam",
    function(x, ...)
{
    if (!.controlled(x))
        stop("'bpstop' not available; instance from outside BiocParallel?")
    if (!bpisup(x))
        stop("cluster already stopped")
    tryCatch(stopCluster(bpbackend(x)), error=function(err) {
        txt <- sprintf("failed to stop %s cluster: %s",
                       sQuote(class(bpbackend(x))[[1]]), conditionMessage(err))
        stop(paste(strwrap(txt, exdent=2), collapse="\n"), call.=FALSE)
    })
    bpbackend(x) <- .NULLcluster()
    invisible(x)
})

setMethod(bpisup, "SnowParam",
    function(x, ...)
{
    length(bpbackend(x)) != 0L
})

setMethod(bpbackend, "SnowParam",
    function(x, ...)
{
    x$cluster
})

setReplaceMethod("bpbackend", c("SnowParam", "cluster"),
    function(x, ..., value)
{
    x$cluster <- value
    x
})

setReplaceMethod("bplog", c("SnowParam", "logical"),
    function(x, ..., value)
{
    if (x$.controlled) {
        x$log <- value 
        if (bpisup(x)) {
            bpstop(x)
            bpstart(x)
        }
        x
    } else {
        stop("'bplog' not available; instance from outside BiocParallel?")
    }
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - evaluation
###

setMethod(bplapply, c("ANY", "SnowParam"),
    function(X, FUN, ..., BPRESUME=getOption("BiocParallel.BPRESUME", FALSE),
    BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    ## no scheduling -> serial evaluation 
    if (!bpschedule(BPPARAM))
        return(bplapply(X, FUN, ..., BPPARAM=SerialParam()))
    if (!bpisup(BPPARAM)) {
        BPPARAM <- bpstart(BPPARAM, length(X))
        on.exit(bpstop(BPPARAM))
    }
    cl <- bpbackend(BPPARAM)
    argfun <- function(i) c(list(X[[i]]), list(...))
    bpdynamicClusterApply(cl, FUN, length(X), names(X), argfun, BPPARAM) 
})

setMethod(bpiterate, c("ANY", "ANY", "SnowParam"),
    function(ITER, FUN, ..., BPPARAM=bpparam())
{
    warning("SnowParam not supported for bpiterate; using SerialParam")
    ITER <- match.fun(ITER)
    FUN <- match.fun(FUN)
    bpiterate(ITER, FUN, ..., BPPARAM=SerialParam()) 
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Getters / Setters
###

.controlled <-
    function(x)
{
    x$.controlled
}

setMethod("bpstopOnError", "SnowParam",
    function(x, ...)
{
    x$stopOnError
})

setReplaceMethod("bpstopOnError", c("SnowParam", "logical"),
    function(x, ..., value)
{
    x$stopOnError <- value 
    x 
})

setMethod("bplog", "SnowParam",
    function(x, ...)
{
    x$log
})

setReplaceMethod("bplog", c("SnowParam", "logical"),
    function(x, ..., value)
{
    x$log <- value 
    x
})

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

setMethod("bpthreshold", "SnowParam",
    function(x, ...)
{
    x$threshold
})

setReplaceMethod("bpthreshold", c("SnowParam", "character"),
    function(x, ..., value)
{
    nms <- c("TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL")
    if (!value %in% nms)
        stop(paste0("'value' must be one of ",
             paste(sQuote(nms), collapse=", ")))
    x$threshold <- .THRESHOLD(value) 
    x
})

setMethod("bplogdir", "SnowParam",
    function(x, ...)
{
    x$logdir
})

setReplaceMethod("bplogdir", c("SnowParam", "character"),
    function(x, ..., value)
{
    x$logdir <- value 
    if (is.null(msg <- .valid.SnowParam.log(x))) 
        x
    else 
        stop(msg)
})

setMethod("bpresultdir", "SnowParam",
    function(x, ...)
{
    x$resultdir
})

setReplaceMethod("bpresultdir", c("SnowParam", "character"),
    function(x, ..., value)
{
    x$resultdir <- value 
    if (is.null(msg <- .valid.SnowParam.result(x))) 
        x
})
### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion methods for SOCK and MPI clusters 
###

### parallel::SOCKcluster types

setOldClass(c("SOCKcluster", "cluster"))

stopCluster.SOCKcluster <-
    parallel:::stopCluster.default

setAs("SOCKcluster", "SnowParam",
    function(from)
{
    .clusterargs <- list(spec=length(from),
        type=sub("cluster$", "", class(from)[1L]))
    .SnowParam(.clusterargs=.clusterargs, cluster=from, 
        .controlled=FALSE, workers=length(from))
})

### MPIcluster

setOldClass(c("spawnedMPIcluster", "MPIcluster", "cluster"))

setAs("spawnedMPIcluster", "SnowParam",
    function(from)
{
    .clusterargs <- list(spec=length(from),
        type=sub("cluster", "", class(from)[1L]))
    .SnowParam(.clusterargs=.clusterargs, cluster=from, .controlled=FALSE,
               workers=length(from))
})
