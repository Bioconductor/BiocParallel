setMethod("bpvectorize", c("ANY", "ANY"),
    function(FUN, ..., BPREDO=list(), BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    function(...)
        bpvec(FUN=FUN, ..., BPREDO=BPREDO, BPPARAM=BPPARAM)
})

setMethod("bpvectorize", c("ANY", "missing"),
    function(FUN, ..., BPREDO=list(), BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    bpvectorize(FUN, ..., BPREDO=BPREDO, BPPARAM=BPPARAM)
})
