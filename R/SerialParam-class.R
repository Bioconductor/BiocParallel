.SerialParam <- setClass("SerialParam",
    representation(),
    prototype(),
    contains = "BiocParallelParam")

.SerialParamSingleton <- .SerialParam()

SerialParam <- function() .SerialParamSingleton

## methods are usually implemented as c("ANY", "ANY", "ANY") in
## corresponding generic.R file
