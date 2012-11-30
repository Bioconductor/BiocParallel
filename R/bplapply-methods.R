setMethod(bplapply, c("ANY", "ANY", "ANY"),
    function(X, FUN, ..., param)
{
    FUN <- match.fun(FUN)
    lapply(X, FUN, ...)
})

setMethod(bplapply, c("ANY", "ANY", "missing"),
    function(X, FUN, ..., param)
{
    FUN <- match.fun(FUN)
    param <- registered()[[1]]
    bplapply(X, FUN, ..., param=param)
})
