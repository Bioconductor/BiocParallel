.SerialParam <- setRefClass("SerialParam",
  contains="BiocParallelParam",
  fields=list(),
)

SerialParam <- function(catch.errors = TRUE) {
  .SerialParam(catch.errors = catch.errors)
}

## control

setMethod(bpworkers, "SerialParam", function(x, ...) 1L)

setMethod(bpisup, "SerialParam", function(x, ...) TRUE)

setMethod(bplapply, c("ANY", "SerialParam"),
  function(X, FUN, ..., BPPARAM) {
    FUN <- match.fun(FUN)
    if (BPPARAM$catch.errors) {
      wrap = function(.FUN, ...) try(do.call(.FUN, list(...)))
      results = lapply(X, wrap, .FUN = FUN, ...)
      is.error = vapply(results, inherits, logical(1L), what = "try-error")
      if (any(is.error))
        LastError$store(obj = X, results = results, is.error = is.error, throw.error = TRUE)
      return(results)
    } else {
      return(lapply(X, FUN, ...))
    }
  }
)
