message("Testing BiocParallelParam")

test_BiocParallelParam <-
    function()
{
    ## BiocParallelParam is a virtual class
    checkException(BiocParallel:::.BiocParallelParam(), silent=TRUE)

    ## minimal non-virtual class & constructor
    .A <- setRefClass("A", contains = "BiocParallelParam")
    A <- function(...) {
        prototype <- .prototype_update(.BiocParallelParam_prototype, ...)
        do.call(.A, prototype)
    }

    ## no arg constructor
    checkTrue(validObject(A()))

    ## non-default inherited slot
    checkIdentical("WARN", bpthreshold(A(threshold = "WARN")))

    ## workers (specified as character()) more than tasks
    checkException(
        validObject(A(workers = rep("a", 3L), tasks = 2L)),
        silent = TRUE
    )
}
