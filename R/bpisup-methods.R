setMethod(bpisup, "ANY", function(param, ...) FALSE)

setMethod(bpisup, "missing",
    function(param, ...)
{
    param <- registered()[[1]]
    bpisup(param, ...)
})

