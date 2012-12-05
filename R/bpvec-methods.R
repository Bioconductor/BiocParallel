setMethod(bpvec, c("ANY", "ANY", "ANY"),
    function(X, FUN, ..., param)
{
    FUN <- match.fun(FUN)
    FUN(X, ...)
})

setMethod(bpvec, c("ANY", "ANY", "BiocParallelParam"),
    function(X, FUN, ..., param)
{
    FUN <- match.fun(FUN)

    tasks <- length(X)
    workers <- min(tasks, bpworkers(param))
    si <- .splitIndex(tasks, workers)
    ans <- bplapply(si, function(i, x, ...) {
        FUN(X[i], ...)
    }, X=X, ..., param=param)
    do.call(c, ans)
})

setMethod(bpvec, c("ANY", "ANY", "missing"),
    function(X, FUN, ..., param)
{
    FUN <- match.fun(FUN)
    param <- registered()[[1]]
    bpvec(X, FUN, ..., param=param)
})
