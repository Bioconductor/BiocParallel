setMethod(bpvectorize, c("ANY", "ANY"),
    function(FUN, ..., BPPARAM)
{
    FUN <- match.fun(FUN)
    function(...) bpvec(FUN=FUN, ..., BPPARAM=BPPARAM)
})

setMethod(bpvectorize, c("ANY", "missing"),
    function(FUN, ..., BPPARAM)
{
    FUN <- match.fun(FUN)
    x <- registered()[[1]]
    bpvectorize(FUN, ..., BPPARAM=x)
})
