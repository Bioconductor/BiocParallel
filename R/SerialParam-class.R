.SerialParamSingleton <- setClass("SerialParam",
    representation(),
    prototype(),
    contains = "BiocParallelParam")()

SerialParam <- function() .SerialParamSingleton

## control

setMethod(bpworkers, "SerialParam", function(param, ...) 1L)

setMethod(bpisup, "SerialParam", function(param, ...) TRUE)
