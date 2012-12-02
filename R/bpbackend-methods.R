setMethod(bpbackend, "missing",
    function(param, ...)
{
    param <- registered()[[1]]
    bpbackend(param, ...)
})

setReplaceMethod("bpbackend", c("missing", "ANY"),
    function(param, ..., value)
{
    param <- registered()[[1]]
    bpbackend(param, ...) <- value
    param
})

