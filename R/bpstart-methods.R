setMethod(bpstart, "ANY", function(param, ...) invisible(param))

setMethod(bpstart, "missing",
    function(param, ...)
{
    param <- registered()[[1]]
    bpstart(param, ...)
})
