### =========================================================================
### bpvec methods 
### -------------------------------------------------------------------------

## MulticoreParam has a dedicated bpvec() method all others use
## bpvec,ANY,BiocParallelParam. bpvec() dispatches to bplapply()
## where errors and logging are handled.

setMethod(bpvec, c("ANY", "missing"),
    function(X, FUN, ..., AGGREGATE=c, BPREDO=list(), BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)
    bpvec(X, FUN, ..., AGGREGATE=AGGREGATE, BPREDO=BPREDO, BPPARAM=BPPARAM)
})

setMethod(bpvec, c("ANY", "BiocParallelParam"),
    function(X, FUN, ..., AGGREGATE=c, BPREDO=list(), BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)

    if (is.na(bpworkers(BPPARAM)))
        stop("'bpworkers' must be set in your backend to use bpvec")

    si <- .splitX(seq_along(X), bpworkers(BPPARAM), bptasks(BPPARAM))
    ans <- bplapply(si, FUN, ..., BPREDO=BPREDO, BPPARAM=BPPARAM) 
    do.call(AGGREGATE, ans)
})

