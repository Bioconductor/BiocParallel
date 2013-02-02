setMethod(bplapply, c("ANY", "ANY"),
    function(X, FUN, ..., BPPARAM)
{
    FUN <- match.fun(FUN)
    lapply(X, FUN, ...)
})

setMethod(bplapply, c("ANY", "missing"),
    function(X, FUN, ..., BPPARAM)
{
    FUN <- match.fun(FUN)
    x <- registered()[[1]]
    bplapply(X, FUN, ..., BPPARAM=x)
})
