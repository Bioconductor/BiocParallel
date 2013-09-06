setMethod(bplapply, c("ANY", "missing"),
    function(X, FUN, ..., resume=FALSE, BPPARAM) {
    FUN <- match.fun(FUN)
    x <- registered()[[1]]
    bplapply(X, FUN, ..., resume=resume, BPPARAM=x)
})

setMethod(bplapply, c("ANY", "BiocParallelParam"), function(X, FUN, ..., resume=FALSE, BPPARAM) {
  bpmapply(FUN, X, MoreArgs = list(...), SIMPLIFY=FALSE, USE.NAMES=FALSE, resume=resume, BPPARAM=BPPARAM)
})

# this is useful for an rapply
# setMethod(bplapply, c(BPPARAM = "list"), function(X, FUN, ..., BPPARAM) {
#     if (!all(vapply(BPPARAM, is, logical(1L), "BiocParallelParam")))
#       stop("All elements in 'BPPARAM' must be BicoParallelParam objects")
#     if (!length(BPPARAM))
#       stop("'length(BPPARAM)' must be >= 1")
#
#     if (length(BPPARAM) >= 2L) {
#       myFun = function(X, ..., .BPPARAM) bplapply(X, FUN, ..., BPPARAM=.BPPARAM)
#       return (bplapply(X, myFun, ..., .BPPARAM = BPPARAM[[1L]],
#                        BPPARAM = tail(BPPARAM, -1L)))
#     }
#     bplapply(X, FUN, ..., BPPARAM = BPPARAM[[1L]])
# })
