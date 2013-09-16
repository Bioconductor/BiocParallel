setMethod(bplapply, c("ANY", "missing"),
    function(X, FUN, ..., resume=getOption("BiocParallel.resume", FALSE), BPPARAM) {
    FUN <- match.fun(FUN)
    x <- registered()[[1L]]
    bplapply(X, FUN, ..., resume=resume, BPPARAM=x)
})

setMethod(bplapply, c("ANY", "BiocParallelParam"), function(X, FUN, ..., resume=getOption("BiocParallel.resume", FALSE), BPPARAM) {
  bpmapply(FUN, X, MoreArgs = list(...), SIMPLIFY=FALSE, USE.NAMES=FALSE, resume=resume, BPPARAM=BPPARAM)
})
