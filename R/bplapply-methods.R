setMethod(bplapply, c("ANY", "missing"),
    function(X, FUN, ..., BPPARAM) {
    FUN <- match.fun(FUN)
    x <- registered()[[1]]
    bplapply(X, FUN, ..., BPPARAM=x)
})

setMethod(bplapply, c("ANY", "BPPARAM"), function(X, FUN, ..., BPPARAM) {
  bpmapply(FUN, X, MoreArgs = list(...), SIMPLIFY=FALSE, USE.NAMES=FALSE, BPPARAM=BPPARAM)
})

# setMethod(bplapply, c("LastError", "missing"),
#     function(X, FUN, ..., BPPARAM) {
#     FUN <- match.fun(FUN)
#     x <- registered()[[1]]
#     bplapply(X, FUN, ..., BPPARAM=x)
# })

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

# setMethod(bplapply, c("LastError", "BiocParallelParam"),
#   function(X, FUN, ..., BPPARAM) {
#     FUN = match.fun(FUN)
#     obj = X$obj
#     if (is.null(obj))
#       stop("LastError is empty")
#     results = X$results
#     is.error = X$is.error

#     # Note that LastError gets updated by this call
#     pars = c(list(X = obj[is.error], FUN = FUN, BPPARAM = BPPARAM), ...)
#     next.try = try(do.call(bplapply, pars))

#     if (inherits(next.try, "try-error")) {
#       # merge partial runs
#       results[is.error] = LastError$results
#       is.error[is.error] = LastError$is.error

#       # update error object
#       LastError$results = results
#       LastError$is.error = is.error
#       # throw error message
#       stop(as.character(next.try))
#     } else {
#       # no errors left
#       # cleanup and return complete list of results
#       LastError$reset()
#       return(replace(results, is.error, next.try))
#     }
#   }
# )
