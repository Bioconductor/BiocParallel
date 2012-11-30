setMethod(bpvec, c("ANY", "ANY", "ANY"),
    function(X, FUN, ..., param)
{
    FUN <- match.fun(FUN)
    FUN(X, ...)
})

setMethod(bpvec, c("ANY", "ANY", "missing"),
    function(X, FUN, ..., param)
{
    FUN <- match.fun(FUN)
    param <- registered()[[1]]
    bpvec(X, FUN, ..., param=param)
})
