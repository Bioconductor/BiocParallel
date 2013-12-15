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
    bpmapply(FUN, X, MoreArgs=list(...), SIMPLIFY=FALSE, USE.NAMES=FALSE,
        BPRESUME=BPRESUME, BPPARAM=BPPARAM)
})

setMethod(bplapply, c(BPPARAM="list"),
    function(X, FUN, ..., BPPARAM)
{
    if (!all(as.character(lapply(BPPARAM, is, "BiocParallelParam"))))
        stop("All elements in 'BPPARAM' must be BicoParallelParam objects")
    if (length(BPPARAM) == 0L)
        stop("'length(BPPARAM)' must be > 0")
    myBPPARAM <- BPPARAM[[1L]]
    BPPARAM <- tail(BPPARAM, -1L)
    if (length(BPPARAM) > 0L) {
        myFUN <- function(...) FUN(..., BPPARAM=BPPARAM)
    } else myFUN <- FUN
    if (length(BPPARAM) == 1L)
        BPPARAM <- BPPARAM[[1L]]
    bplapply(X, myFUN, ..., BPPARAM=myBPPARAM)
})
