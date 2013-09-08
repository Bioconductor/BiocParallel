.DoparParam <- setRefClass("DoparParam",
    contains="BiocParallelParam",
    fields=list()
)

DoparParam <- function(catch.errors=TRUE, store.dump=FALSE) {
  .DoparParam(catch.errors=catch.errors, store.dump=store.dump)
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

setMethod(bpmapply, c("ANY", "DoparParam"),
  function(FUN, ..., MoreArgs = NULL, SIMPLIFY = TRUE, USE.NAMES = TRUE, resume=FALSE, BPPARAM) {
    FUN <- match.fun(FUN)
    if (resume)
      return(.resume(FUN=FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY, USE.NAMES=USE.NAMES, BPPARAM=BPPARAM))
    if (!bpisup(BPPARAM))
      return(Recall(FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY, USE.NAMES=USE.NAMES, resume=resume, BPPARAM=SerialParam()))

    ddd = list(...)
    MoreArgs = as.list(MoreArgs)
    len = vapply(ddd, length, integer(1L))
    if (!all(len == len[1L])) {
      max.len = max(len)
      if (any(max.len %% len))
        warning("longer argument not a multiple of length of vector")
      ddd = lapply(ddd, rep_len, length.out = max.len)
    }
    i <- NULL                           # quieten R CMD check
    results = foreach(i = seq_len(len[[1L]]), .errorhandling = "pass") %dopar% {
      do.call("FUN", args = c(lapply(ddd, "[[", i), MoreArgs))
    }
    is.error = vapply(results, inherits, logical(1L), what="error")
    if (any(is.error)) {
      if (BPPARAM$catch.errors)
        LastError$store(results=results, is.error=is.error, throw.error=TRUE)
      stop(results[[head(which(is.error), 1L)]])
    }

    return(.RenameSimplify(results, list(...), USE.NAMES, SIMPLIFY))
})
