### =========================================================================
### bpiterate methods
### -------------------------------------------------------------------------

## All params have dedicated bpiterate() methods.

setMethod("bpiterate", c("ANY", "ANY", "missing"),
    function(ITER, FUN, ..., BPREDO=list(),
             BPPARAM=bpparam(), BPOPTIONS=bpoptions())
{
    ITER <- match.fun(ITER)
    FUN <- match.fun(FUN)
    bpiterate(ITER, FUN, ..., BPREDO = BPREDO,
              BPPARAM=BPPARAM, BPOPTIONS = BPOPTIONS)
})

## TODO: support BPREDO
.bpiterate_impl <-
    function(ITER, FUN, ..., REDUCE, init, reduce.in.order = FALSE,
              BPREDO = list(), BPPARAM = bpparam(), BPOPTIONS=bpoptions())
{
    ## Required API
    ##
    ## - BiocParallelParam()
    ## - bpschedule(), bpisup(), bpstart(), bpstop()
    ## - .sendto, .recvfrom, .recv, .close
    ITER <- match.fun(ITER)
    FUN <- match.fun(FUN)

    if (missing(REDUCE)) {
        if (!missing(init))
            stop("REDUCE must be provided when 'init' is given")
    }

    ARGS <- list(...)

    manager <- structure(list(), class="iterate") # dispatch
    .bpinit(
        manager = manager,
        ITER = ITER,
        FUN = FUN,
        ARGS = ARGS,
        BPPARAM = BPPARAM,
        BPOPTIONS = BPOPTIONS,
        BPREDO = BPREDO,
        init = init,
        REDUCE = REDUCE,
        reduce.in.order = reduce.in.order
    )
}
