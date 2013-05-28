.SerialParamSingleton <-
    setRefClass("SerialParam",
                contains="BiocParallelParam")(workers=1)

SerialParam <- function() .SerialParamSingleton

## control

setMethod(bpworkers, "SerialParam", function(x, ...) 1L)

setMethod(bpisup, "SerialParam", function(x, ...) TRUE)
