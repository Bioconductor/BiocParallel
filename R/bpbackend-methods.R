setMethod("bpbackend", "missing",
    function(x)
{
    x <- registered()[[1]]
    bpbackend(x)
})

setReplaceMethod("bpbackend", c("missing", "ANY"),
    function(x, value)
{
    x <- registered()[[1]]
    bpbackend(x) <- value
    x
})

