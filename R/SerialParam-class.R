.SerialParamSingleton <- setClass("SerialParam",
    representation(),
    prototype(),
    contains = "BiocParallelParam")()

SerialParam <- function() .SerialParamSingleton

## control

setMethod(bpworkers, "SerialParam", function(x, ...) 1L)

setMethod(bpisup, "SerialParam", function(x, ...) TRUE)
