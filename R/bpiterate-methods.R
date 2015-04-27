### =========================================================================
### bpiterate methods 
### -------------------------------------------------------------------------

## All params have dedicated bpiterate() methods.

setMethod(bpiterate, c("ANY", "ANY", "missing"),
    function(ITER, FUN, ..., BPPARAM=bpparam())
{
    ITER <- match.fun(ITER)
    FUN <- match.fun(FUN)
    bpiterate(ITER, FUN, ..., BPPARAM=BPPARAM)
})

setMethod(bpiterate, c("ANY", "ANY", "BiocParallelParam"),
    function(ITER, FUN, ..., BPPARAM=bpparam())
{
    ITER <- match.fun(ITER)
    FUN <- match.fun(FUN)
    bpiterate(ITER, FUN, ..., BPPARAM=BPPARAM)
})
