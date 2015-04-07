setMethod(bpvec, c("ANY", "ANY"),
    function(X, FUN, ..., AGGREGATE=c,  BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)
    FUN(X, ..., AGGREGATE=AGGREGATE)
})

setMethod(bpvec, c("ANY", "BiocParallelParam"),
    function(X, FUN, ..., AGGREGATE=c, BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)

    if (is.na(bpworkers(BPPARAM)))
        stop("'bpworkers' must be set in your backend to use bpvec")

    si <- .splitX(seq_along(X), bpworkers(BPPARAM), bptasks(BPPARAM))
    ans <- bplapply(si, function(.i, .X, .FUN, ...) {
        .FUN(.X[.i], ...)
    }, .X=X, .FUN=FUN, ..., BPPARAM=BPPARAM)
    do.call(AGGREGATE, ans)
})

setMethod(bpvec, c("ANY", "missing"),
    function(X, FUN, ..., AGGREGATE=c, BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)

    bpvec(X, FUN, ..., AGGREGATE=AGGREGATE, BPPARAM=BPPARAM)
})
