setMethod(bpvectorize, c("ANY", "ANY"),
    function(FUN, ..., param)
{
    FUN <- match.fun(FUN)
    function(...)
        bpvec(FUN=FUN, ..., param=param)
})

setMethod(bpvectorize, c("ANY", "missing"),
    function(FUN, ..., param)
{
    FUN <- match.fun(FUN)
    param <- registered()[[1]]
    bpvectorize(FUN, ..., param=param)
})
