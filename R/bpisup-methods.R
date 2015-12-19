setMethod("bpisup", "ANY", function(x) FALSE)

setMethod("bpisup", "missing",
    function(x)
{
    x <- registered()[[1]]
    bpisup(x)
})

