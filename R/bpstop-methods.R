setMethod(bpstop, "ANY", function(param, ...) invisible(param))

setMethod(bpstop, "missing",
    function(param, ...)
{
    param <- registered()[[1]]
    bpstop(param, ...)
})

