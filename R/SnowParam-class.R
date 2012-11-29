setOldClass(c("SOCKcluster", "cluster"))

.SnowParam <- setClass("SnowParam",
    representation(
        cluster="SOCKcluster"),
    prototype(),
    "BiocParallelParam")

SnowParam <-
    function(spec = 0L, type, ...)
{
    if (missing(type))
        type <- parallel:::getClusterOption("type")
    cluster <- parallel::makeCluster(spec, type, ...)
    .SnowParam(cluster=cluster)
}

setMethod(bplapply, c("ANY", "ANY", "SnowParam"),
    function(X, FUN, ..., param)
{
    FUN <- match.fun(FUN)
    parLapply(param@cluster, X, FUN, ...)
})

setMethod(bpvec, c("ANY", "ANY", "SnowParam"),
    function(X, FUN, ..., param)
{
    FUN <- match.fun(FUN)
    if (!length(param@cluster))
        return(FUN(X, ...))

    n <- length(X)
    nodes <- min(n, length(param@cluster))
    si <- splitIndices(n, nodes)
    ans <- bplapply(si, function(i, ...) FUN(X[i], ...), ..., param=param)
    do.call(c, ans)
})

setMethod(show, "SnowParam",
    function(object)
{
    callNextMethod()
    show(object@cluster)
})
