.DoparParam <- setClass("DoparParam",
    representation(),
    prototype(),
    contains = "BiocParallelParam")

.DoparParamSingleton <- .DoparParam()

DoparParam <- function() .DoparParamSingleton

.doParBackendRegistered <- function() {
    "package:foreach" %in% search() &&
        foreach::getDoParRegistered() &&
            foreach::getDoParName() != "doSEQ" &&
                foreach::getDoParWorkers() > 1
}

setMethod(bplapply, c("ANY", "ANY", "DoparParam"),
    function(X, FUN, ..., param)
{
    FUN <- match.fun(FUN)
    ## If no parallel backend is registered for foreach, fall back to
    ## the serial backend.
    if (!.doParBackendRegistered())
        return(bplapply(X, FUN, ..., param=SerialParam()))

    ans <- foreach(x=X) %dopar% FUN(x, ...)
    setNames(ans, names(X))
})

setMethod(bpvec, c("ANY", "ANY", "DoparParam"),
    function(X, FUN, ..., param)
{
    FUN <- match.fun(FUN)
    ## If no parallel backend is registered for foreach, fall back to
    ## the serial backend.
    if (!.doParBackendRegistered())
        return(bpvec(X, FUN, ..., param=SerialParam()))

    n <- length(X)
    nodes <- min(n, getDoParWorkers())
    si <- splitIndices(n, nodes)
    sx <- lapply(si, function(i) X[i])
    ans <- bplapply(sx, function(x, ...) FUN(x, ...), ..., param=param)
    do.call(c, ans)
})
