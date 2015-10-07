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

## FIXME: bpvec should assume FUN is vectorized and apply FUN() to a
##        group of X elements, not individual elements. Currently
##        bplapply (Snow, Multicore) lapply's over the elements. bpvec 
##        should have its own implementation where the 'argfun' argument to 
##        .bpdynamicClusterApply is not lapply().
setMethod(bpvec, c("ANY", "BiocParallelParam"),
    function(X, FUN, ..., AGGREGATE=c, BPREDO=list(), BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)
    if (is.na(bpworkers(BPPARAM)))
        stop("'bpworkers' must be set in your backend to use bpvec")
    ## tasks = 0 is even split of X over workers
    if (is(BPPARAM, "DoparParam") || is(BPPARAM, "BatchJobsParam"))
        X <- .splitX(X, bpworkers(BPPARAM), 0)
    else if (bptasks(BPPARAM) != 0)
        bptasks(BPPARAM) <- 0
    ans <- bplapply(X, FUN, ..., BPREDO=BPREDO, BPPARAM=BPPARAM) 
    do.call(AGGREGATE, ans)
})

