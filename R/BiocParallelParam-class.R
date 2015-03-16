### =========================================================================
### BiocParallelParam objects
### -------------------------------------------------------------------------

.BiocParallelParam <- setRefClass("BiocParallelParam",
    contains="VIRTUAL",
    fields=list(
        workers="ANY",
        catch.errors="logical", ## BatchJobs, DoPar
        stopOnError="logical"), ## SnowParam
    methods=list(
      initialize = function(..., 
          workers=0, 
          catch.errors=TRUE,
          stopOnError=FALSE)
      {
          initFields(workers=workers, catch.errors=catch.errors, 
                     stopOnError=stopOnError)
          callSuper(...)
      },
      show = function() {
          cat("class:", class(.self), "\n")
          cat("bpworkers:", bpworkers(.self), "\n")
      })
)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Validity
###

setValidity("BiocParallelParam", function(object)
{
    msg <- NULL
    if (is.numeric(bpworkers(object))) 
        if (length(bpworkers(object)) != 1L || bpworkers(object) < 0)
            msg <- c(msg, "'workers' must be integer(1) and >= 0")

    if (is.character(bpworkers(object)))
        if (length(bpworkers(object)) < 1L)
            msg <- c(msg, "length(bpworkers(BPPARAM)) must be > 0") 

    if (!.isTRUEorFALSE(object$catch.errors))
        msg <- c(msg, "'catch.errors' must be TRUE or FALSE")

    if (!.isTRUEorFALSE(bpstopOnError(object)))
        msg <- c(msg, "'bpstopOnError(BPPARAM)' must be logical(1)")

    if (is.null(msg)) TRUE else msg
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Getters / Setters
###

setMethod(bpworkers, "BiocParallelParam",
   function(x, ...)
{
    x$workers
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Helpers
###

## taken from S4Vectors
.isTRUEorFALSE <- function (x) {
    is.logical(x) && length(x) == 1L && !is.na(x)
}

bpok <- function(x) {
    if (!is(x, "list"))
        stop("'x' must be a list")

    lapply(x, function(elt) !is(elt, "try-error"))
}
