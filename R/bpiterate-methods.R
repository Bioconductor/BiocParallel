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

## TODO: support BPREDO
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

    missing.init <- missing(init)
    if (missing(REDUCE)) {
        if (reduce.in.order)
            stop("REDUCE must be provided when 'reduce.in.order = TRUE'")
        if (!missing.init)
            stop("REDUCE must be provided when 'init' is given")
        REDUCE_ <- c
    }else{
        REDUCE <- match.fun(REDUCE)
        errorValue <- NULL
        REDUCE_ <- function(x, y){
            ## initial value
            if(missing.init){
                missing.init <<- FALSE
                return(y[[1]])
            }
            ## when error occurs and cannot auto combine
            if(!is.null(errorValue))
                return(errorValue)

            ## check if the error can be combined with the results
            if(inherits(y[[1]], "bperror")){
                if(missing.init || !identical(init, list()) || !identical(REDUCE, c)){
                    errorValue <<- y[[1]]
                    return(errorValue)
                }else{
                    REDUCE(x, y)
                }
            }else{
                REDUCE(x, y[[1]])
            }

        }
        if(missing.init)
            init <- list()
    }

    ARGS <- list(...)

    manager <- structure(list(), class="iterate") # dispatch
    res <- bpinit(
        manager = manager,
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
