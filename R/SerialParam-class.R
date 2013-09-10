.SerialParam <- setRefClass("SerialParam",
  contains="BiocParallelParam",
  fields=list()
)

SerialParam <- function(catch.errors=TRUE) {
  .SerialParam(catch.errors=catch.errors)
}

## control

setMethod(bpworkers, "SerialParam", function(x, ...) 1L)

setMethod(bpisup, "SerialParam", function(x, ...) TRUE)

setMethod(bpmapply, c("function", "SerialParam"),
  function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE, resume=FALSE, BPPARAM) {
    FUN <- match.fun(FUN)
    if (resume)
      return(.resume(FUN=FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY, USE.NAMES=USE.NAMES, BPPARAM=BPPARAM))

    if (BPPARAM$catch.errors) {
      FUN = .composeTry(FUN)
      results = mapply(FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=FALSE, USE.NAMES=USE.NAMES)

      is.error = vapply(results, inherits, logical(1L), what="remote-error") 
      if (any(is.error))
        LastError$store(results=results, is.error=is.error, throw.error=TRUE)
      
      return(.simplify(results, SIMPLIFY=SIMPLIFY))
    } 

    mapply(FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=FALSE, USE.NAMES=USE.NAMES)
})
