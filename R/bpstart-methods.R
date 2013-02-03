setMethod(bpstart, "ANY", function(x, ...) invisible(x))

setMethod(bpstart, "missing",
    function(x, ...)
{
    x <- registered()[[1]]
    bpstart(x, ...)
})
