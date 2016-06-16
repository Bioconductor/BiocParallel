### =========================================================================
### bpvec methods 
### -------------------------------------------------------------------------

## bpvec() dispatches to bplapply() where errors and logging are
## handled.

setMethod("bpvec", c("ANY", "BiocParallelParam"),
    function(X, FUN, ..., AGGREGATE=c, BPREDO=list(), BPPARAM=bpparam())
{
    if (!length(X))
        return(list())

    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)

    if (!bpschedule(BPPARAM))
        return(bpvec(X, FUN, ..., AGGREGATE=AGGREGATE, BPREDO=BPREDO,
               BPPARAM=SerialParam()))

    si <- .splitX(seq_along(X), bpnworkers(BPPARAM), bptasks(BPPARAM))
    bptasks(BPPARAM) <- 0

    idx <- .redo_index(si, BPREDO)
    if (any(idx))
        si <- si[idx]

    ## FIXME: 'X' sent to all workers, but ith worker only needs X[i]
    FUN1 <- function(i, ...) FUN(X[i], ...)
    res <- bptry(bplapply(si, FUN1, ..., BPREDO=BPREDO[idx], BPPARAM=BPPARAM))

    if (any(idx)) {
        BPREDO[idx] <- res
        res <- BPREDO
    }

    if (is(res, "error") || !all(bpok(res)))
        stop(.error_bplist(res))

    if (any(lengths(res) != lengths(si)))
        stop(.error("length(FUN(X)) not equal to length(X)", "bpvec_error"))

    do.call(AGGREGATE, res)
})

setMethod("bpvec", c("ANY", "missing"),
    function(X, FUN, ..., AGGREGATE=c, BPREDO=list(), BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)
    bpvec(X, FUN, ..., AGGREGATE=AGGREGATE, BPREDO=BPREDO, BPPARAM=BPPARAM)
})

setMethod("bpvec", c("ANY", "list"),
    function(X, FUN, ..., BPREDO=list(), BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)

    if (!all(vapply(BPPARAM, inherits, logical(1), "BiocParallelParam")))
        stop("All elements in 'BPPARAM' must be BiocParallelParam objects")
    if (length(BPPARAM) == 0L)
        stop("'length(BPPARAM)' must be > 0")

    myFUN <- if (length(BPPARAM) > 1L) {
        param <- BPPARAM[-1]
        if (length(param) == 1L)
            function(...) FUN(..., BPPARAM=param[[1]])
        else
            function(...) FUN(..., BPPARAM=param)
    } else FUN

    bpvec(X, myFUN, ..., BPREDO=BPREDO, BPPARAM=BPPARAM[[1]])
})
