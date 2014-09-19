setMethod(bplapply, c("ANY", "missing"),
    function(X, FUN, ..., BPRESUME=getOption("BiocParallel.BPRESUME", FALSE),
        BPTRACE=TRUE, BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    bplapply(X, FUN, ..., BPRESUME=BPRESUME, BPTRACE=BPTRACE, BPPARAM=BPPARAM)
})

setMethod(bplapply, c("ANY", "BiocParallelParam"),
    function(X, FUN, ..., BPRESUME=getOption("BiocParallel.BPRESUME", FALSE),
        BPTRACE=TRUE, BPPARAM=bpparam())
{
    bpmapply(FUN, X, MoreArgs=list(...), SIMPLIFY=FALSE,
        BPRESUME=BPRESUME, BPTRACE=BPTRACE, BPPARAM=BPPARAM)
})

setMethod(bplapply, c("ANY", "list"),
    function(X, FUN, ..., BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    if (!all(vapply(BPPARAM, is, logical(1), "BiocParallelParam")))
        stop("All elements in 'BPPARAM' must be BiocParallelParam objects")
    if (length(BPPARAM) == 0L)
        stop("'length(BPPARAM)' must be > 0")
    myFUN <- if (length(BPPARAM) > 1L)
        function(...) FUN(..., BPPARAM=BPPARAM[-1L])
    else FUN
    bplapply(X, myFUN, ..., BPPARAM=BPPARAM[[1L]])
})
