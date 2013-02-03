setOldClass(c("SOCKcluster", "cluster"))

.SnowParam <- setClass("SnowParam",
    representation(
        .clusterargs="list",
        cluster="SOCKcluster"),
    prototype(),
    "BiocParallelParam")

.nullCluster <- function(type)
{
    if (type == "FORK")
        type <- "PSOCK"
    makeCluster(0L, type)
}

SnowParam <-
    function(workers = 0L, type, ...)
{
    if (missing(type))
        type <- parallel:::getClusterOption("type")
    .clusterargs <- lapply(c(list(spec=workers, type=type), list(...)), force)
    cluster <- .nullCluster(type)
    .SnowParam(.clusterargs=.clusterargs, cluster=cluster)
}

setAs("SOCKcluster", "SnowParam", function(from) {
    .SnowParam(cluster=from, .controlled=FALSE)
})

## control

setMethod(bpworkers, "SnowParam",
    function(x, ...)
{
    if (bpisup(x))
        length(bpbackend(x))
    else
        x@.clusterargs$spec
})

setMethod(bpstart, "SnowParam",
    function(x, ...)
{
    if (!.controlled(x))
        stop("'bpstart' not available; instance from outside BiocParallel?")
    bpbackend(x) <- do.call(makeCluster, x@.clusterargs)
    invisible(x)
})

setMethod(bpstop, "SnowParam",
    function(x, ...)
{
    if (!.controlled(x))
        stop("'bpstop' not available; instance from outside BiocParallel?")
    stopCluster(x@cluster)
    bpbackend(x) <- .nullCluster(x@.clusterargs$type)
    invisible(x)
})

setMethod(bpisup, "SnowParam",
    function(x, ...)
{
    length(bpbackend(x)) != 0
})

setMethod(bpbackend, "SnowParam",
    function(x, ...)
{
    x@cluster
})

setReplaceMethod("bpbackend", c("SnowParam", "SOCKcluster"),
    function(x, ..., value)
{
    x@cluster <- value
    x
})

## evaluation

setMethod(bplapply, c("ANY", "SnowParam"),
    function(X, FUN, ..., BPPARAM)
{
    FUN <- match.fun(FUN)
    if (!bpisup(BPPARAM)) {
        BPPARAM <- bpstart(BPPARAM)
        on.exit(bpstop(BPPARAM))
    }
    parLapply(bpbackend(BPPARAM), X, FUN, ...)
})

setMethod(show, "SnowParam",
    function(object)
{
    callNextMethod()
    if (bpisup(object))
        show(bpbackend(object))
    else {
        cat("cluster 'spec': ", object@.clusterargs$spec,
            "; 'type': ", object@.clusterargs$type, "\n", sep="")
    }
})
