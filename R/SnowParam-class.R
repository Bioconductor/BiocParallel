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
        RNGseed="ANY",
        log="logical",
        threshold="ANY",
        logdir="character",
        resultdir="character"),
    methods=list(
        initialize = function(..., 
            .controlled=TRUE,
            RNGseed=NULL,
            log=FALSE,
            threshold="INFO",
            logdir=NA_character_,
            resultdir=NA_character_)
        { 
            initFields(.controlled=.controlled, RNGseed=RNGseed,
                       log=log, threshold=threshold, 
                       logdir=logdir, resultdir=resultdir)
            callSuper(...)
        },
        show = function() {
            callSuper()
            cat("  bpworkers:", bpworkers(.self), 
                   "; bptasks:", bptasks(.self),
                   "; bpRNGseed:", bpRNGseed(.self),
                   "; bpisup:", bpisup(.self), "\n", sep="")
            cat("  bplog:", bplog(.self),
                   "; bpthreshold:", names(bpthreshold(.self)),
                   "; bplogdir:", bplogdir(.self))
            cat("  bpstopOnError:", bpstopOnError(.self),
                   "; bpprogressbar:", bpprogressbar(.self), "\n", sep="")
            cat("  bpresultdir:", bpresultdir(.self), "\n", sep="")
            cat("cluster type:", .clusterargs$type, "\n")
        })
)

SnowParam <- function(workers=snowWorkers(), type=c("SOCK", "MPI", "FORK"), 
                      tasks=0L, catch.errors=TRUE, stop.on.error=FALSE, 
                      progressbar=FALSE, RNGseed=NULL, log=FALSE, 
                      threshold="INFO", logdir=NA_character_, 
                      resultdir=NA_character_, ...)
{
    type <- match.arg(type)
    if (!catch.errors)
        warning("'catch.errors' has been deprecated")
    if (!type %in% c("SOCK", "MPI", "FORK"))
        stop("type must be one of 'SOCK', 'MPI' or 'FORK'")
    if (any(type %in% c("MPI", "FORK")) && is(workers, "character"))
        stop("'workers' must be integer(1) when 'type' is MPI or FORK") 

    args <- c(list(spec=workers, type=type), list(...)) 
    # FIXME I don't think this is required, lists always inflict a copy
    .clusterargs <- lapply(args, force)
    cluster <- .NULLcluster()
 
    x <- .SnowParam(.clusterargs=.clusterargs, cluster=cluster, 
                    .controlled=TRUE, workers=workers, tasks=as.integer(tasks), 
                    catch.errors=catch.errors, stop.on.error=stop.on.error, 
                    progressbar=progressbar, RNGseed=RNGseed, log=log, 
                    threshold=.THRESHOLD(threshold), logdir=logdir, 
                    resultdir=resultdir)
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

    dir <- bplogdir(object) 
    if (length(dir) > 1L || !is(dir, "character"))
        msg <- c(msg, "'bplogdir(BPPARAM)' must be character(1)")

    if (!is.na(dir) && !bplog(object))
        msg <- c(msg, "'log' must be TRUE when 'logdir' is provided")
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

    if (.controlled(object)) {
        threshold <- bpthreshold(object)
        if (length(threshold) != 1L) 
            return(c(msg, "'bpthreshold(BPPARAM)' must be character(1)"))

        nms <- c("TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL")
        if (!names(threshold) %in% nms) {
            return(c(msg, paste0("'bpthreshold(BPPARAM)' must be one of ", 
                   paste(sQuote(nms), collapse=", "))))
        }
        msg <- c(msg, 
                 .valid.SnowParam.log(object),
                 .valid.SnowParam.result(object))
    }

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

setReplaceMethod("bpworkers", c("SnowParam", "numeric"),
    function(x, ..., value)
{
    value <- as.integer(value)
    if (value > snowWorkers())
        stop("'value' exceeds available workers detected by snowWorkers()")
 
    x$tasks <- value 
    x 
})

setReplaceMethod("bpworkers", c("SnowParam", "character"),
    function(x, ..., value)
{
    if (length(value) > snowWorkers())
        stop("'value' exceeds available workers detected by snowWorkers()")
 
    x$tasks <- value 
    x 
})

setMethod(bpstart, "SnowParam",
    function(x, lenX = bpworkers(x), ...)
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
    if (lenX > 0)
        x$.clusterargs$spec <- min(bpworkers(x), lenX) 
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
    if (bplog(x))
        .initiateLogging(x)
    clusterSetRNGStream(bpbackend(x), bpRNGseed(x))
    invisible(x)
})

setMethod(bpstop, "SnowParam",
    function(x, ...)
{
    if (!.controlled(x))
        stop("'bpstop' not available; instance from outside BiocParallel?")
    if (!bpisup(x))
        return(x)

    tryCatch(stopCluster(bpbackend(x)), 
        error=function(err) {
            txt <- sprintf("failed to stop %s cluster: %s",
                           sQuote(class(bpbackend(x))[[1]]), 
                           conditionMessage(err))
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

setMethod(bpRNGseed, "SnowParam",
    function(x, ...)
{
    x$RNGseed 
})

setReplaceMethod("bpRNGseed", c("SnowParam", "ANY"),
    function(x, ..., value)
{
    if (!is.null(value))
        value <- as.integer(value)
    x$RNGseed <- value 
    x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - evaluation
###

setMethod(bplapply, c("ANY", "SnowParam"),
    function(X, FUN, ..., BPREDO=list(), BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    if (length(BPREDO)) {
        if (all(idx <- !bpok(BPREDO)))
            stop("no error detected in 'BPREDO'")
        if (length(BPREDO) != length(X))
            stop("length(BPREDO) must equal length(X)")
        message("Resuming previous calculation ... ")
        X <- X[idx]
    }
    nms <- names(X)

    if (!length(X))
        return(list())
    if (!bpschedule(BPPARAM)) 
        return(bplapply(X, FUN, ..., BPPARAM=SerialParam()))

    if (bplog(BPPARAM) || bpstopOnError(BPPARAM))
        FUN <- .composeTry(FUN, TRUE)
    else
        FUN <- .composeTry(FUN, FALSE)

    ## split into tasks 
    X <- .splitX(X, bpworkers(BPPARAM), bptasks(BPPARAM))
    argfun <- function(i) c(list(X[[i]]), list(FUN=FUN), list(...))

    ## start cluster
    if (!bpisup(BPPARAM)) {
        BPPARAM <- bpstart(BPPARAM, length(X))
        on.exit(bpstop(BPPARAM), TRUE)
    }

    ## progress bar
    progress <- .progress(active=bpprogressbar(BPPARAM))  
    progress$init(length(X))
    on.exit(progress$term(), TRUE)

    if (bplog(BPPARAM) || bpstopOnError(BPPARAM) || bpprogressbar(BPPARAM))
        res <- bpdynamicClusterApply(bpbackend(BPPARAM), lapply, 
                                     length(X), argfun, BPPARAM, progress)
    else
        res <- parallel:::dynamicClusterApply(bpbackend(BPPARAM), lapply, 
                                              length(X), argfun)
    if (!is.null(res)) {
        res <- do.call(c, res, quote=TRUE)
        names(res) <- nms
    }

    if (length(BPREDO)) {
        BPREDO[idx] <- res
        BPREDO 
    } else res
})

setMethod(bpiterate, c("ANY", "ANY", "SnowParam"),
    function(ITER, FUN, ..., BPPARAM=bpparam())
{
    ITER <- match.fun(ITER)
    FUN <- match.fun(FUN)

    if (!bpschedule(BPPARAM))
        return(bpiterate(ITER, FUN, ..., BPPARAM=SerialParam()))
    if (bplog(BPPARAM) || bpstopOnError(BPPARAM))
        FUN <- .composeTry(FUN, TRUE)
    else
        FUN <- .composeTry(FUN, FALSE)

    ## start cluster
    if (!bpisup(BPPARAM)) {
        BPPARAM <- bpstart(BPPARAM)
        on.exit(bpstop(BPPARAM), TRUE)
    }

    bpdynamicClusterIterate(bpbackend(BPPARAM), FUN, ITER, ..., BPPARAM=BPPARAM)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Getters / Setters
###

.controlled <-
    function(x)
{
    x$.controlled
}

setMethod("bplog", "SnowParam",
    function(x, ...)
{
    x$log
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
