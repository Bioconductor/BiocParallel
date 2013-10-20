setMethod(bpvec, c("ANY", "ANY"),
    function(X, FUN, ..., AGGREGATE=c,  BPPARAM)
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)
    FUN(X, ..., AGGREGATE=AGGREGATE)
})

setMethod(bpvec, c("ANY", "BiocParallelParam"),
    function(X, FUN, ..., AGGREGATE=c, BPPARAM)
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)

    tasks <- length(X)
    workers <- min(tasks, bpworkers(BPPARAM))
    if (is.na(workers))
        stop("'bpworkers' must be set in your backend to use bpvec")

    si <- .splitIndices(tasks, workers)
    ans <- bplapply(si, function(i, X, FUN, ...) {
        FUN(X[i], ...)
    }, X, FUN, ..., BPPARAM=BPPARAM)
    do.call(AGGREGATE, ans)
})

setMethod(bpvec, c("ANY", "missing"),
    function(X, FUN, ..., AGGREGATE=c, BPPARAM)
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)

    x <- registered()[[1]]
    bpvec(X, FUN, ..., AGGREGATE=AGGREGATE, BPPARAM=x)
})
