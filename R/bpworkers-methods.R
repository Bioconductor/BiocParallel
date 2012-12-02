setMethod(bpworkers, "missing",
    function(param, ...)
{
    param <- registered()[[1]]
    bpworkers(param, ...)
})
