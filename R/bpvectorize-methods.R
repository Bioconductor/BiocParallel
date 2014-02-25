setMethod(bpvectorize, c("ANY", "ANY"),
    function(FUN, ..., BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    function(...)
        bpvec(FUN=FUN, ..., BPPARAM=BPPARAM)
})

setMethod(bpvectorize, c("ANY", "missing"),
    function(FUN, ..., BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    bpvectorize(FUN, ..., BPPARAM=BPPARAM)
})
