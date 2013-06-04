.SerialParamSingleton <- setRefClass("SerialParam",
    contains="BiocParallelParam")$new(workers=1L)

SerialParam <- function() .SerialParamSingleton

## control

setMethod(bpworkers, "SerialParam", function(x, ...) 1L)

setMethod(bpisup, "SerialParam", function(x, ...) TRUE)
