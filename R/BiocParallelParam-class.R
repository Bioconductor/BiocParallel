.BiocParallelParam <- setRefClass("BiocParallelParam",
    contains="VIRTUAL",
    fields=list(
      .controlled="logical",
      workers="numeric"),
    methods=list(
      initialize = function(..., workers=0, .controlled=TRUE) {
          initFields(workers=workers, .controlled=.controlled)
          callSuper(...)
      },
      show = function() {
          cat("class: ", class(.self), "; bpisup: ", bpisup(.self),
              "; bpworkers: ", bpworkers(.self), "\n",
              sep="")
      }))

setValidity("BiocParallelParam", function(object)
{
    msg <- NULL
    if (length(bpworkers(object)) != 1L || bpworkers(object) < 0)
        msg <- c(msg, "'workers' must be integer(1) and >= 0")
    if (length(.controlled(object)) != 1L || is.na(.controlled(object)))
        msg <- c(msg, "'.controlled' must be TRUE or FALSE")
    if (is.null(msg)) TRUE else msg
})

.controlled <-
    function(x)
{
    x$.controlled
}

setMethod(bpworkers, "BiocParallelParam",
   function(x, ...)
{
    x$workers
})
