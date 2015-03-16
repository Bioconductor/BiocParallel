### =========================================================================
### SnowParam objects
### -------------------------------------------------------------------------

## 11 February, 2014 -- this is a hack: import'ing
## parallel::stopCluster and then library(snow) overwrites
## parallel::stopCluster.default with snow::stopCluster.default,
## resulting in incorrect dispatch to snow::sendData
stopCluster <- parallel::stopCluster


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
        .clusterargs="list",
        cluster="cluster"),
    methods=list(
        show=function() {
            callSuper()
            cat("cluster type: ", .clusterargs$type, "\n")
    })
)

SnowParam <- function(workers=snowWorkers(), type=c("SOCK", "MPI", "FORK"), 
    stopOnError=FALSE, log=FALSE, threshold="INFO", logdir=character(),
    resultdir=character(), ...)
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
    if (!require(parallel))
        stop("SnowParam class objects require the 'parallel' package")
    if (!.controlled(x))
        stop("'bpstart' not available; instance from outside BiocParallel?")
    if (bpisup(x))
        stop("cluster already started")

    x$.clusterargs$spec <- min(bpworkers(x), tasks) 
    if (bplog(x)) {
        ## worker script in BiocParallel
        if (x$.clusterargs$type == "FORK") {
            bpbackend(x) <- do.call(.bpmakeForkCluster, x$.clusterargs)
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
    x$log <- value 
    if (bpisup(x)) {
        bpstop(x)
        bpstart(x)
    }
    x
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
        BPPARAM <- bpstart(BPPARAM)
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
