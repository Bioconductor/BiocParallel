setClass("BiocParallelParam", representation("VIRTUAL"))

setMethod(show, "BiocParallelParam",
    function(object)
{
    cat("class:", class(object), "\n")
})
