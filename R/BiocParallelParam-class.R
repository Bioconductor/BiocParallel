setClass("BiocParallelParam",
    representation(
        "VIRTUAL",
        .controlled="logical",
        workers="integer"),
    prototype(.controlled=TRUE, workers=0L))

setValidity("BiocParallelParam", function(object) 
{
    msg <- NULL
    if (length(object@workers) != 1L || object@workers < 0)
        msg <- c(msg, "'workers' must be integer(1) and >= 0")
    if (is.null(msg)) TRUE else msg
})

.controlled <-
    function(param)
{
    param@.controlled
}

setMethod(bpworkers, "BiocParallelParam",
   function(param, ...)
{
    param@workers
})

setMethod(show, "BiocParallelParam",
    function(object)
{
    cat("class: ", class(object), "; bpisup: ", bpisup(object),
        "; bpworkers: ", bpworkers(object), "\n",
        sep="")
})
