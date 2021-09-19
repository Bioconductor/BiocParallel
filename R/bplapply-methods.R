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

.dummy_iter <- function(X){
    i <- 0L
    n <- length(X)
    function(){
        if (i < n) {
            i <<- i + 1L
            X[[i]]
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
    FUN <- match.fun(FUN)

    if (!length(X))
        return(.rename(list(), X))

    ## which need to be redone?
    redo_index <- .redo_index(X, BPREDO)
    if (any(redo_index)) {
        compute_element <- which(redo_index)
        X <- X[compute_element]
    } else {
        compute_element <- NULL
    }
    nms <- names(X)

    ## split into tasks
    X <- .splitX(X, bpnworkers(BPPARAM), bptasks(BPPARAM), redo_index)

    ## iterator for X
    ITER <- .dummy_iter(X)

    ARGS <- list(...)

    manager <- structure(list(), class="lapply") # dispatch
    res <- bpinit(
        manager = manager,
        X = X,
        FUN = FUN,
        ARGS = ARGS,
        BPPARAM = BPPARAM
    )

    if (length(compute_element)) {
        BPREDO[compute_element] <- res
        res <- BPREDO
    }

    if (!is.null(res)) {
        names(res) <- nms
    }

    if (!all(bpok(res)) && bpstopOnError(BPPARAM))
        stop(.error_bplist(res))

    res
}
