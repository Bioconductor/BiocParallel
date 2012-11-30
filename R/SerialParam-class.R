SerialParam <- setClass("SerialParam",
    representation(),
    prototype(),
    contains = "BiocParallelParam")

## methods are usually implemented as c("ANY", "ANY", "ANY") in
## corresponding generic.R file
