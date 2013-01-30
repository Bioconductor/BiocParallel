setMethod(bpvec, c("ANY", "ANY", "ANY"),
    function(X, FUN, ..., AGGREGATE=c,  param)
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)
    FUN(X, ..., AGGREGATE=AGGREGATE)
})

setMethod(bpvec, c("ANY", "ANY", "BiocParallelParam"),
    function(X, FUN, ..., AGGREGATE=c, param)
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)

    tasks <- length(X)
    workers <- min(tasks, bpworkers(param))
    si <- .splitIndices(tasks, workers)
    ans <- bplapply(si, function(i, X, FUN, ...) {
        FUN(X[i], ...)
    }, X, FUN, ..., param=param)
    do.call(AGGREGATE, ans)
})

setMethod(bpvec, c("ANY", "ANY", "missing"),
    function(X, FUN, ..., AGGREGATE=c, param)
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)

    param <- registered()[[1]]
    bpvec(X, FUN, ..., AGGREGATE=AGGREGATE, param=param)
})
