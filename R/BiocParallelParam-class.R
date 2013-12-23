.BiocParallelParam <- setRefClass("BiocParallelParam",
    contains="VIRTUAL",
    fields=list(
      .controlled="logical",
      workers="numeric",
      catch.errors="logical"),
    methods=list(
      initialize = function(..., workers=0, .controlled=TRUE,
        catch.errors=TRUE)
      {
          initFields(workers=workers, .controlled=.controlled,
                     catch.errors=catch.errors)
          callSuper(...)
      },
      show = function() {
          cat("class: ", class(.self), "; bpisup: ", bpisup(.self),
              "; bpworkers: ", bpworkers(.self), "; catch.errors: ",
              catch.errors, "\n", sep="")
      })
)

setValidity("BiocParallelParam", function(object)
{
    msg <- NULL
    if (length(bpworkers(object)) != 1L || bpworkers(object) < 0)
        msg <- c(msg, "'workers' must be integer(1) and >= 0")
    if (length(.controlled(object)) != 1L || is.na(.controlled(object)))
        msg <- c(msg, "'.controlled' must be TRUE or FALSE")
    if (length(object$catch.errors) != 1L || is.na(object$catch.errors))
        msg <- c(msg, "'catch.errors' must be TRUE or FALSE")
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
