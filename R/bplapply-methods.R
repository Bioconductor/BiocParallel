### =========================================================================
### bplapply methods
### -------------------------------------------------------------------------

## All params have dedicated bplapply methods.

setMethod("bplapply", c("ANY", "missing"),
    function(X, FUN, ..., BPREDO=list(), BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    bplapply(X, FUN, ..., BPREDO=BPREDO, BPPARAM=BPPARAM)
})

setMethod("bplapply", c("ANY", "list"),
    function(X, FUN, ..., BPREDO=list(), BPPARAM=bpparam())
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
    bplapply(X, myFUN, ..., BPREDO=BPREDO, BPPARAM=BPPARAM[[1]])
})

.dummy_ITER <- function(X, redo_index){
    i <- 0L
    n <- length(redo_index)
    function(){
        if (i < n) {
            i <<- i + 1L
            X[[redo_index[i]]]
        }else{
            NULL
        }
    }
}

.bplapply_impl <-
    function(X, FUN, ..., BPREDO = list(), BPPARAM = bpparam())
{
    ## abstract 'common' implementation using accessors only
    ##
    ## Required API:
    ##
    ## - BiocParallelParam()
    ## - bpschedule(), bpisup(), bpstart(), bpstop()
    ## - .send_to, .recv_any, .send, .recv, .close


    ## which need to be redone?
    redo_index <- .redo_index(X, BPREDO)
    if (any(redo_index)) {
        redo_index <- which(redo_index)
    } else {
        redo_index <- seq_along(X)
    }

    ## iterator for X
    ITER <- .dummy_ITER(X, redo_index)

    res <- .bpiterate_impl(ITER = ITER,
                           FUN = FUN,
                           ...,
                           init = list(),
                           REDUCE = function(x, y) append(x, list(y)),
                           reduce.in.order = TRUE,
                           BPPARAM = BPPARAM,
                           value.index = redo_index)

    # if (!is.null(res)) {
    #     res <- do.call(unlist, list(res, recursive=FALSE))
    #     names(res) <- nms
    # }

    if (length(redo_index)) {
        BPREDO[redo_index] <- res
        res <- BPREDO
    }

    res
}
