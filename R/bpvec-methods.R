## This one method will work for any param type. Methods should only
## be defined for specific param classes for the purpose of improving
## efficiency.
setMethod(bpvec, c("ANY", "ANY", "ANY"),
    function(X, FUN, ..., param)
{
    FUN <- match.fun(FUN)

    nodes <- min(length(X), bpworkers(param))
    if (nodes <= 1)
        return(FUN(X, ...))

    sx <- splitList(X, nodes)
    ans <- bplapply(sx, function(x, ...) FUN(x, ...), ..., param=param)
    do.call(c, ans)

})

setMethod(bpvec, c("ANY", "ANY", "missing"),
    function(X, FUN, ..., param)
{
    FUN <- match.fun(FUN)
    param <- registered()[[1]]
    bpvec(X, FUN, ..., param=param)
})
