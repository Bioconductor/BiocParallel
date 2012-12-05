setMethod(bpschedule, "ANY", function(param, ...) TRUE)

setMethod(bpschedule, "missing",
    function(param, ...)
{
    param <- registered()[[1]]
    bpschedule(param, ...)
})

