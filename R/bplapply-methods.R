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

.dummy_ITER <- function(X){
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

    if (!bpschedule(BPPARAM) || length(X) == 1L || bpnworkers(BPPARAM) == 1L) {
        param <- as(BPPARAM, "SerialParam")
        return(bplapply(X, FUN, ..., BPREDO=BPREDO, BPPARAM=param))
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

    ## which need to be redone?
    redo_index <- .redo_index(X, BPREDO)
    if (any(redo_index)) {
        X <- X[redo_index]
        compute_element <- which(redo_index)
    } else {
        compute_element <- seq_along(X)
    }
    nms <- names(X)

    ## split into tasks
    X <- .splitX(X, bpnworkers(BPPARAM), bptasks(BPPARAM))

    ## iterator for X
    ITER <- .dummy_ITER(X)

    ARGS <- list(...)

    cls <- structure(list(), class="iterate")
    res <- bploop(cls, # dispatch
           ITER, FUN, ARGS, BPPARAM,
           init = list(),
           REDUCE = c,
           reduce.in.order = TRUE)

    # res <- bpinit(X = X,
    #               FUN = FUN,
    #               ARGS = ARGS,
    #               BPPARAM = BPPARAM,
    #        init = list(),
    #        REDUCE = c,
    #        reduce.in.order = TRUE
    #        )




    if (!is.null(res)) {
        # res <- do.call(unlist, list(res, recursive=FALSE))
        names(res) <- nms
    }

    if (length(compute_element)) {
        BPREDO[compute_element] <- res
        res <- BPREDO
    }

    res
}
