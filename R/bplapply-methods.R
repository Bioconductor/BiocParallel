setMethod(bplapply, c("ANY", "missing"),
    function(X, FUN, ..., BPRESUME=getOption("BiocParallel.BPRESUME", FALSE),
        BPPARAM)
{
    FUN <- match.fun(FUN)
    x <- registered()[[1L]]
    bplapply(X, FUN, ..., BPRESUME=BPRESUME, BPPARAM=x)
})

setMethod(bplapply, c("ANY", "BiocParallelParam"),
    function(X, FUN, ..., BPRESUME=getOption("BiocParallel.BPRESUME", FALSE),
        BPPARAM)
{
    bpmapply(FUN, X, MoreArgs=list(...), SIMPLIFY=FALSE,
        BPRESUME=BPRESUME, BPPARAM=BPPARAM)
})

setMethod(bplapply, c("ANY", "list"),
    function(X, FUN, ..., BPPARAM)
{
    if (!all(vapply(BPPARAM, is, logical(1), "BiocParallelParam")))
        stop("All elements in 'BPPARAM' must be BicoParallelParam objects")
    if (length(BPPARAM) == 0L)
        stop("'length(BPPARAM)' must be > 0")
    myFUN <- if (length(BPPARAM) > 1L)
        function(...) FUN(..., BPPARAM=BPPARAM[-1L])
    else FUN
    bplapply(X, myFUN, ..., BPPARAM=BPPARAM[[1L]])
})
