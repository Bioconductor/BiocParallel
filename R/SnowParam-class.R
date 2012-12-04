setOldClass(c("SOCKcluster", "cluster"))

.SnowParam <- setClass("SnowParam",
    representation(
        .clusterargs="list",
        cluster="SOCKcluster"),
    prototype(),
    "BiocParallelParam")

.nullCluster <- function(type)
    makeCluster(0L, type)

SnowParam <-
    function(workers = 0L, type, ...)
{
    if (missing(type))
        type <- parallel:::getClusterOption("type")
    .clusterargs <- lapply(c(list(spec=workers, type=type), list(...)), force)
    cluster <- .nullCluster(type)
    .SnowParam(.clusterargs=.clusterargs, cluster=cluster)
}

## control

setMethod(bpworkers, "SnowParam",
    function(param, ...)
{
    if (bpisup(param))
        length(bpbackend(param))
    else
        param@.clusterargs$spec
})

setMethod(bpstart, "SnowParam",
    function(param, ...)
{
    bpbackend(param) <- do.call(makeCluster, param@.clusterargs)
    invisible(param)
})

setMethod(bpstop, "SnowParam",
    function(param, ...)
{
    stopCluster(param@cluster)
    bpbackend(param) <- .nullCluster(param@.clusterargs$type)
    invisible(param)
})

setMethod(bpisup, "SnowParam",
    function(param, ...)
{
    length(bpbackend(param)) != 0
})

setMethod(bpbackend, "SnowParam",
    function(param, ...)
{
    param@cluster
})

setReplaceMethod("bpbackend", c("SnowParam", "SOCKcluster"),
    function(param, ..., value)
{
    param@cluster <- value
    param
})

## evaluation

setMethod(bplapply, c("ANY", "ANY", "SnowParam"),
    function(X, FUN, ..., param)
{
    FUN <- match.fun(FUN)
    if (!bpisup(param)) {
        param <- bpstart(param)
        on.exit(bpstop(param))
    }
    parLapply(bpbackend(param), X, FUN, ...)
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
