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
        param <- SerialParam(stop.on.error=bpstopOnError(BPPARAM),
                             log=bplog(BPPARAM),
                             threshold=bpthreshold(BPPARAM),
                             logdir = bplogdir(BPPARAM),
                             progressbar=bpprogressbar(BPPARAM))
        return(bplapply(X, FUN, ..., BPREDO=BPREDO, BPPARAM=param))
    }

    ## which need to be redone?
    redo_index <- .redo_index(X, BPREDO)
    if (any(redo_index)) {
        X <- X[redo_index]
        compute_element <- redo_index
    } else {
        compute_element <- rep(TRUE, length(X))
    }
    nms <- names(X)

    ## start / stop cluster
    if (!bpisup(BPPARAM)) {
        if (is(BPPARAM, "MulticoreParam"))
            BPPARAM <- TransientMulticoreParam(BPPARAM)
        BPPARAM <- bpstart(BPPARAM, length(X))
        on.exit(bpstop(BPPARAM), TRUE)
    }

    ## FUN
    FUN <- .composeTry(
        FUN, bplog(BPPARAM), bpstopOnError(BPPARAM),
        timeout=bptimeout(BPPARAM), exportglobals=bpexportglobals(BPPARAM)
    )

    ## split into tasks
    X <- .splitX(X, bpnworkers(BPPARAM), bptasks(BPPARAM))
    BPRNGSEEDS <- .rng_seeds_by_task(BPPARAM, compute_element, lengths(X))
    ARGFUN <- function(i)
        c(
            list(X=X[[i]]), list(FUN=FUN), list(...),
            list(BPRNGSEED = BPRNGSEEDS[[i]])
        )

    cls <- structure(list(), class="lapply")
    res <- bploop(cls, X, .rng_lapply, ARGFUN, BPPARAM)

    if (!is.null(res)) {
        res <- do.call(unlist, list(res, recursive=FALSE))
        names(res) <- nms
    }

    if (any(redo_index)) {
        BPREDO[redo_index] <- res
        res <- BPREDO
    }

    if (!all(bpok(res)))
        stop(.error_bplist(res))

    res
}
