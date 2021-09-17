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
             BPPARAM = bpparam(), value.index = NULL)
{
    ## Required API
    ##
    ## - BiocParallelParam()
    ## - bpschedule(), bpisup(), bpstart(), bpstop()
    ## - .sendto, .recvfrom, .recv, .close
    ITER <- match.fun(ITER)
    FUN <- match.fun(FUN)

    if (missing(REDUCE)) {
        if (reduce.in.order)
            stop("REDUCE must be provided when 'reduce.in.order = TRUE'")
        if (!missing(init))
            stop("REDUCE must be provided when 'init' is given")
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
    ARGFUN <- function(value) c(list(value), list(...))

    ## FIXME: handle errors via bpok()
    res <- bploop(structure(list(), class="iterate"), # dispatch
           ITER, FUN, ARGFUN, BPPARAM, REDUCE, init, reduce.in.order)


    if (!all(bpok(res)) && bpstopOnError(BPPARAM)){
        if(!is.null(value.index)){
            error_res <- res
            res <- vector("list", value.index[length(res)])
            res[value.index[seq_along(error_res)]] <- error_res
        }
        stop(.error_bplist(res))
    }
    res
}
