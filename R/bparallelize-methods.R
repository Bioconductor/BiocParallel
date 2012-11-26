setMethod(bparallelize, c("ANY", "ANY"),
    function(FUN, ..., param)
{
    FUN <- match.fun(FUN)
    function(...)
        bpvec(FUN=FUN, ..., param=param)
})

setMethod(bparallelize, c("ANY", "missing"),
    function(FUN, ..., param)
{
    FUN <- match.fun(FUN)
    param <- registered()[[1]]
    bparallelize(FUN, ..., param=param)
})
