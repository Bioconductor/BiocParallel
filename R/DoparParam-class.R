.DoparParam <- setRefClass("DoparParam",
    contains="BiocParallelParam",
    fields=list(),
)

DoparParam <- function(catch.errors = TRUE) {
  .DoparParam(catch.errors = catch.errors)
}

## control

setMethod(bpworkers, "DoparParam",
  function(x, ...) {
    if (bpisup(x))
      getDoParWorkers()
    else 0L
})

setMethod(bpisup, "DoparParam", function(x, ...) {
    "package:foreach" %in% search() && getDoParRegistered() &&
      getDoParName() != "doSEQ" && getDoParWorkers() > 1L
})

## evaluation

setMethod(bplapply, c("ANY", "DoparParam"),
    function(X, FUN, ..., BPPARAM) {
    FUN <- match.fun(FUN)
    ## If no parallel backend is registered for foreach, fall back to
    ## the serial backend.
    if (!bpisup(BPPARAM))
        return(bplapply(X, FUN, ..., BPPARAM=SerialParam()))

    x <- NULL                           # quieten R CMD check
    results <- foreach(x=X, .errorhandling = "pass") %dopar% FUN(x, ...)
    is.error = vapply(results, inherits, logical(1L), what = "error")
    if (any(is.error)) {
      if (BPPARAM$catch.errors)
        LastError$store(obj = X, results = results, is.error = is.error, throw.error = TRUE)
      stop(results[[head(which(is.error), 1L)]])
    }
    return(results)
})
