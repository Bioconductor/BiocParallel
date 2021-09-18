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
    ITER_ <- function(){
        list(ITER())
    }
    FUN <- match.fun(FUN)

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

    if (!bpschedule(BPPARAM) || bpnworkers(BPPARAM) == 1L) {
        param <- as(BPPARAM, "SerailParam")
        return(bpiterate(ITER, FUN, ..., REDUCE=REDUCE, init=init,
                         BPPARAM=param))
    }

    ## start / stop cluster
    if (!bpisup(BPPARAM)) {
        BPPARAM <- bpstart(BPPARAM)
        on.exit(bpstop(BPPARAM), TRUE)
    }

    ## FUN
    FUN <- .composeTry(
        FUN, bplog(BPPARAM), bpstopOnError(BPPARAM),
        timeout=bptimeout(BPPARAM), exportglobals=bpexportglobals(BPPARAM)
    )
    ARGS <- list(...)

    ## FIXME: handle errors via bpok()
    cls <- structure(list(), class="iterate")
    res <- bploop(cls, # dispatch
                  ITER_, FUN, ARGS, BPPARAM,
                  init = init,
                  REDUCE = REDUCE_,
                  reduce.in.order = reduce.in.order)


    if (!all(bpok(res)) && bpstopOnError(BPPARAM)){
        stop(.error_bplist(res))
    }
    res
}
