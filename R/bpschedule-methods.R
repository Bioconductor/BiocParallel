setMethod("bpschedule", "ANY", function(x) TRUE)

setMethod("bpschedule", "missing",
    function(x)
{
    x <- registered()[[1]]
    bpschedule(x)
})

