.SerialParam <- setRefClass("SerialParam",
  contains="BiocParallelParam",
  fields=list()
)

SerialParam <- function(catch.errors=TRUE, store.dump=FALSE) {
  .SerialParam(catch.errors=catch.errors, store.dump=FALSE)
}

## control

setMethod(bpworkers, "SerialParam", function(x, ...) 1L)

setMethod(bpisup, "SerialParam", function(x, ...) TRUE)

setMethod(bpmapply, c("function", "SerialParam"),
  function(FUN, ..., MoreArgs = NULL, SIMPLIFY = TRUE, USE.NAMES = TRUE, BPPARAM) {
    FUN <- match.fun(FUN)
    if (BPPARAM$catch.errors) {
      wrap = function(.FUN, ...) .try(.FUN(...), debug=BPPARAM$store.dump)
      results = mapply(wrap, ..., MoreArgs=c(list(.FUN = FUN), MoreArgs), SIMPLIFY=FALSE, USE.NAMES=USE.NAMES)
      is.error = vapply(results, inherits, logical(1L), what="try-error") 
      if (any(is.error))
        LastError$store(args=list(...), results=results, is.error=is.error, MoreArgs=MoreArgs, throw.error=TRUE)
      return(results)
    } 
    mapply(FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY, USE.NAMES=USE.NAMES)
})
