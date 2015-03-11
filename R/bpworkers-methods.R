setMethod(bpworkers, "missing",
    function(x, ...)
{
    x <- registered()[[1]]
    bpworkers(x, ...)
})
