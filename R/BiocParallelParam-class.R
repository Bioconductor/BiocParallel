BiocParallelParam <-
    setRefClass("BiocParallelParam",
                contains="VIRTUAL",
                fields=list(
                controlled="logical",
                workers="numeric"),
                methods=list(initialize=function(..., workers=0, controlled=TRUE) {
                    callSuper(..., workers=workers, controlled=controlled)
                }))

setValidity("BiocParallelParam", function(object)
{
    msg <- NULL
    if (length(object$workers) != 1L || object$workers < 0)
        msg <- c(msg, "'workers' must be integer(1) and >= 0")
    if (length(object$controlled) != 1L || is.na(object$controlled))
        msg <- c(msg, "'controlled' must be TRUE or FALSE")
    if (is.null(msg)) TRUE else msg
})

.controlled <-
    function(x)
{
    x$controlled
}

setMethod(bpworkers, "BiocParallelParam",
   function(x, ...)
{
    x$workers
})

setMethod(show, "BiocParallelParam",
    function(object)
{
    cat("class: ", class(object), "; bpisup: ", bpisup(object),
        "; bpworkers: ", bpworkers(object), "\n",
        sep="")
})
