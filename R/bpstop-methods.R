setMethod(bpstop, "ANY", function(x, ...) invisible(x))

setMethod(bpstop, "missing",
    function(x, ...)
{
    x <- registered()[[1]]
    bpstop(x, ...)
})

