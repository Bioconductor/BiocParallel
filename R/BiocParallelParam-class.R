.BiocParallelParam <- setRefClass("BiocParallelParam",
    contains="VIRTUAL",
    fields=list(
      .controlled = "logical",
      workers = "numeric",
      catch.errors = "logical"),
    methods=list(
      initialize = function(..., workers=0, .controlled=TRUE, catch.erros = FALSE) {
          initFields(workers=workers, .controlled=.controlled, catch.errors = catch.errors)
          callSuper(...)
      },
      show = function() {
          cat("class: ", class(.self), "; bpisup: ", bpisup(.self),
              "; bpworkers: ", bpworkers(.self), "; catch.errors: ", catch.errors, "\n",
              sep="")
      }))

setValidity("BiocParallelParam", function(object) {
    msg <- character(0L)
    if (length(bpworkers(object)) != 1L || bpworkers(object) < 0)
        msg <- c(msg, "'workers' must be integer(1) and >= 0")
    if (length(.controlled(object)) != 1L || is.na(.controlled(object)))
        msg <- c(msg, "'.controlled' must be TRUE or FALSE")
    if (length(catch.errors) != 1L || is.na(catch.errors))
        msg <- c(msg, "'catch.errors' must be TRUE or FALSE")
    if (length(msg)) msg else TRUE
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
