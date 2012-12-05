setClass("BiocParallelParam",
    representation("VIRTUAL", workers="integer"),
    prototype(workers=0L))

setValidity("BiocParallelParam", function(object) 
{
    msg <- NULL
    if (length(object@workers) != 1L || object@workers < 0)
        msg <- c(msg, "'workers' must be integer(1) and >= 0")
    if (is.null(msg)) TRUE else msg
})

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
