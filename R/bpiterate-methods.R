### =========================================================================
### bpiterate methods
### -------------------------------------------------------------------------

## All params have dedicated bpiterate() methods.

setMethod("bpiterate", c("ANY", "ANY", "missing"),
    function(ITER, FUN, ..., BPPARAM=bpparam())
{
    ITER <- match.fun(ITER)
    FUN <- match.fun(FUN)
    bpiterate(ITER, FUN, ..., BPPARAM=BPPARAM)
})


.bpiterate_impl <-
    function(ITER, FUN, ..., REDUCE, init, reduce.in.order = FALSE,
             BPPARAM = bpparam())
{
    ## Required API
    ##
    ## - BiocParallelParam()
    ## - bpschedule(), bpisup(), bpstart(), bpstop()
    ## - .sendto, .recvfrom, .recv, .close

    FUN <- match.fun(FUN)

    ITER_ <- function(){
        list(ITER())
    }

    if (missing(REDUCE)) {
        if (reduce.in.order)
            stop("REDUCE must be provided when 'reduce.in.order = TRUE'")
        if (!missing(init))
            stop("REDUCE must be provided when 'init' is given")
        REDUCE_ <- c
    }else{
        REDUCE_ <- function(x, y){
            REDUCE(x, y[[1]])
        }
        if (missing(init))
            init <- list()
    }

    ARGS <- list(...)

    res <- bpinit(
        ITER = ITER_,
        FUN = FUN,
        ARGS = ARGS,
        BPPARAM = BPPARAM,
        init = init,
        REDUCE = REDUCE_,
        reduce.in.order = reduce.in.order
    )

    if (!all(bpok(res)) && bpstopOnError(BPPARAM)){
        stop(.error_bplist(res))
    }
    res
}
