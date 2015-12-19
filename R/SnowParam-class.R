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
    cores <- max(1L, parallel::detectCores() - 2L)
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
        logdir="character",
        resultdir="character"),
    methods=list(
        initialize = function(...,
            .clusterargs=list(spec=0, type="SOCK"),
            .controlled=TRUE,
            RNGseed=NULL,
            logdir=NA_character_,
            resultdir=NA_character_)
        { 
            callSuper(...)
            initFields(.clusterargs=.clusterargs, .controlled=.controlled,
                       RNGseed=RNGseed, logdir=logdir, resultdir=resultdir)
        },
        show = function() {
            callSuper()
            cat("\n",
                "  bpRNGseed: ", bpRNGseed(.self),
                "\n",
                "  bplogdir: ", bplogdir(.self),
                "\n",
                "  bpresultdir: ", bpresultdir(.self),
                "\n",
                "  cluster type: ", .clusterargs$type,
                "\n", sep="")
        })
)

SnowParam <- function(workers=snowWorkers(), 
                      type=c("SOCK", "MPI", "FORK"), tasks=0L, 
                      catch.errors=TRUE, stop.on.error=TRUE, 
                      progressbar=FALSE, RNGseed=NULL, timeout=Inf,
                      log=FALSE, threshold="INFO", logdir=NA_character_, 
                      resultdir=NA_character_, jobname = "BPJOB", ...)
{
    if (!missing(catch.errors))
        warning("'catch.errors' is deprecated, use 'stop.on.error'")

    type <- tryCatch(match.arg(type), error=function(...) {
        stop("'type' must be one of ",
             paste(sQuote(formals("SnowParam")$type), collapse=", "))
    })

    if (type %in% c("MPI", "FORK") && is(workers, "character"))
        stop("'workers' must be integer(1) when 'type' is MPI or FORK") 

    args <- c(list(spec=workers, type=type), list(...)) 
    x <- .SnowParam(.clusterargs=args, cluster=.NULLcluster(), 
                    .controlled=TRUE, workers=workers, tasks=as.integer(tasks), 
                    catch.errors=catch.errors, stop.on.error=stop.on.error, 
                    progressbar=progressbar, RNGseed=RNGseed, timeout=timeout,
                    log=log, threshold=threshold, logdir=logdir, 
                    resultdir=resultdir, jobname=jobname)
    validObject(x)
    x
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Validity
###

.valid.SnowParam.log <- function(object)
{
    msg <- NULL

    dir <- bplogdir(object) 
    if (length(dir) > 1L || !is(dir, "character"))
        msg <- c(msg, "'bplogdir(BPPARAM)' must be character(1)")

    if (!is.na(dir) && !bplog(object))
        msg <- c(msg, "'log' must be TRUE when 'logdir' is given")
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
        if (bpworkers(object) != object$.clusterargs$spec)
            msg <- c(msg, 
                "'bpworkers(BPPARAM)' must equal BPPARAM$.clusterargs$spec")
        msg <- c(msg, 
                 .valid.SnowParam.log(object),
                 .valid.SnowParam.result(object))
    }

    if (is.null(msg)) TRUE else msg
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Getters / Setters
###

setReplaceMethod("bpworkers", c("SnowParam", "numeric"),
    function(x, value)
{
    value <- as.integer(value)
    if (value > snowWorkers())
        stop("'value' exceeds available workers detected by snowWorkers()")
 
    x$workers <- value 
    x$.clusterargs$spec <- value 
    x 
})

setMethod("bpRNGseed", "SnowParam",
    function(x)
{
    x$RNGseed 
})

setReplaceMethod("bpRNGseed", c("SnowParam", "numeric"),
    function(x, value)
{
    x$RNGseed <- as.integer(value)
    x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - control
###

setMethod("bpstart", "SnowParam",
    function(x, lenX = bpworkers(x))
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
                "SnowParam objects require the 'parallel' package")
        })
    }

    ## 'X' has been split into tasks - start minimum number of workers.
    if (lenX > 0) {
        cargs <- x$.clusterargs
        cargs$spec <- min(bpworkers(x), lenX)
    } else stop("cluster not started; no workers specified")

    ## FORK (useRscript not relevant)
    if (x$.clusterargs$type == "FORK") {
        bpbackend(x) <- 
            do.call(.bpmakeForkCluster, c(list(nnodes=cargs$spec), cargs))
    ## SOCK, MPI
    } else {
        cargs$snowlib <- find.package("BiocParallel")
        if (!is.null(cargs$useRscript)) {
            if (!cargs$useRscript)
                cargs$scriptdir <- find.package("BiocParallel")
        }
        bpbackend(x) <- do.call(makeCluster, cargs)
    }

    ## logging
    if (bplog(x)) {
        .initiateLogging(x)
    }

    ## random numbers 
    if (!is.null(bpRNGseed(x))) {
        tryCatch({
            clusterSetRNGStream(bpbackend(x), bpRNGseed(x))
        }, error = function(err) {
               bpstop(x)
               stop(conditionMessage(err), ": problem setting RNG stream") 
        })
    }

    ## timeout 
    if (is.finite(timeout <- bptimeout(x))) {
        tryCatch({
            clusterExport(bpbackend(x), "timeout", env=environment())
        }, error = function(err) {
               bpstop(x)
               stop(conditionMessage(err), ": problem setting worker timeout") 
        })
    }
    invisible(x)
})

setMethod("bpstop", "SnowParam",
    function(x)
{
    if (!.controlled(x))
        stop("'bpstop' not available; instance from outside BiocParallel?")
    if (!bpisup(x))
        return(invisible(x))

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

setMethod("bpisup", "SnowParam",
    function(x)
{
    length(bpbackend(x)) != 0L
})

setMethod("bpbackend", "SnowParam",
    function(x)
{
    x$cluster
})

setReplaceMethod("bpbackend", c("SnowParam", "cluster"),
    function(x, value)
{
    x$cluster <- value
    x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - evaluation
###

setMethod("bplapply", c("ANY", "SnowParam"),
    function(X, FUN, ..., BPREDO=list(), BPPARAM=bpparam())
{

    if (!length(X))
        return(list())
    FUN <- match.fun(FUN)
    if (!bpschedule(BPPARAM) || length(X) == 1L || bpworkers(BPPARAM) == 1L) {
        param <- SerialParam(stop.on.error=bpstopOnError(BPPARAM),
                             log=bplog(BPPARAM),
                             threshold=bpthreshold(BPPARAM))
        return(bplapply(X, FUN, ..., BPPARAM=param))
    }
    if (length(BPREDO)) {
        if (all(idx <- !bpok(BPREDO)))
            stop("no previous error in 'BPREDO'")
        if (length(BPREDO) != length(X))
            stop("length(BPREDO) must equal length(X)")
        message("Resuming previous calculation ... ")
        X <- X[idx]
    }
    nms <- names(X)

    ## split into tasks 
    X <- .splitX(X, bpworkers(BPPARAM), bptasks(BPPARAM))

    ## start / stop cluster
    if (!bpisup(BPPARAM)) {
        BPPARAM <- bpstart(BPPARAM, length(X))
        on.exit(bpstop(BPPARAM), TRUE)
    }

    ## progress bar
    progress <- .progress(active=bpprogressbar(BPPARAM))
    progress$init(length(X))
    on.exit(progress$term(), TRUE)

    FUN <- .composeTry(FUN, bplog(BPPARAM), bpstopOnError(BPPARAM),
                       timeout=bptimeout(BPPARAM))
    argfun <- function(i) c(list(X[[i]]), list(FUN=FUN), list(...))
    res <- bpdynamicClusterApply(bpbackend(BPPARAM), lapply, 
                                 length(X), argfun, BPPARAM, 
                                 progress)

    if (!is.null(res)) {
        res <- do.call(unlist, list(res, recursive=FALSE))
        names(res) <- nms
    }

    if (length(BPREDO)) {
        BPREDO[idx] <- res
        res <- BPREDO 
    }

    if (!all(bpok(res)))
        stop(.error_bplist(res))

    res
})

setMethod("bpiterate", c("ANY", "ANY", "SnowParam"),
    function(ITER, FUN, ..., BPPARAM=bpparam())
{
    ITER <- match.fun(ITER)
    FUN <- match.fun(FUN)

    if (!bpschedule(BPPARAM))
        return(bpiterate(ITER, FUN, ..., BPPARAM=SerialParam()))

    FUN <- .composeTry(FUN, bplog(BPPARAM), bpstopOnError(BPPARAM),
                       timeout=bptimeout(BPPARAM))

    ## start / stop cluster
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

setReplaceMethod("bplog", c("SnowParam", "logical"),
    function(x, value)
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

setReplaceMethod("bpthreshold", c("SnowParam", "character"),
    function(x, value)
{
    x$threshold <- value
    x
})

setMethod("bplogdir", "SnowParam",
    function(x)
{
    x$logdir
})

setReplaceMethod("bplogdir", c("SnowParam", "character"),
    function(x, value)
{
    if (!length(value))
        value <- NA_character_
    x$logdir <- value 
    if (is.null(msg <- .valid.SnowParam.log(x))) 
        x
    else 
        stop(msg)
})

setMethod("bpresultdir", "SnowParam",
    function(x)
{
    x$resultdir
})

setReplaceMethod("bpresultdir", c("SnowParam", "character"),
    function(x, value)
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
