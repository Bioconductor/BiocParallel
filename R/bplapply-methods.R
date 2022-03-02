### =========================================================================
### bplapply methods
### -------------------------------------------------------------------------

## All params have dedicated bplapply methods.

setMethod("bplapply", c("ANY", "missing"),
    function(X, FUN, ..., BPREDO=list(),
             BPPARAM=bpparam(), BPOPTIONS = bpoptions())
{
    FUN <- match.fun(FUN)
    bplapply(X, FUN, ..., BPREDO=BPREDO,
             BPPARAM=BPPARAM, BPOPTIONS = BPOPTIONS)
})

setMethod("bplapply", c("ANY", "list"),
    function(X, FUN, ..., BPREDO=list(),
             BPPARAM=bpparam(), BPOPTIONS = bpoptions())
{
    FUN <- match.fun(FUN)

    if (!all(vapply(BPPARAM, inherits, logical(1), "BiocParallelParam")))
        stop("All elements in 'BPPARAM' must be BiocParallelParam objects")
    if (length(BPPARAM) == 0L)
        stop("'length(BPPARAM)' must be > 0")

    myFUN <- if (length(BPPARAM) > 1L) {
          if (length(param <- BPPARAM[-1]) == 1L)
            function(...) FUN(..., BPPARAM=param[[1]])
          else
            function(...) FUN(..., BPPARAM=param)
        } else FUN
    bplapply(X, myFUN, ..., BPREDO=BPREDO,
             BPPARAM=BPPARAM[[1]], BPOPTIONS = BPOPTIONS)
})

.bplapply_impl <-
    function(X, FUN, ..., BPREDO = list(),
             BPPARAM = bpparam(), BPOPTIONS = bpoptions())
{
    ## abstract 'common' implementation using accessors only
    ##
    ## Required API:
    ##
    ## - BiocParallelParam()
    ## - bpschedule(), bpisup(), bpstart(), bpstop()
    ## - .send_to, .recv_any, .send, .recv, .close
    FUN <- match.fun(FUN)
    BPREDO <- bpresult(BPREDO)

    if (!length(X))
        return(.rename(list(), X))

    ARGS <- list(...)

    manager <- structure(list(), class="lapply") # dispatch
    .bpinit(
        manager = manager,
        X = X,
        FUN = FUN,
        ARGS = ARGS,
        BPPARAM = BPPARAM,
        BPOPTIONS = BPOPTIONS,
        BPREDO = BPREDO
    )
}
