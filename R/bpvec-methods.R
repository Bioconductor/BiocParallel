### =========================================================================
### bpvec methods
### -------------------------------------------------------------------------

## bpvec() dispatches to bplapply() where errors and logging are
## handled.

setMethod("bpvec", c("ANY", "BiocParallelParam"),
    function(X, FUN, ..., AGGREGATE=c, BPREDO=list(),
             BPPARAM=bpparam(), BPOPTIONS = bpoptions())
{
    if (!length(X))
        return(.rename(list(), X))

    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)
    BPREDO <- bpresult(BPREDO)

    if (!bpschedule(BPPARAM)) {
        param <- as(BPPARAM, "SerialParam")
        return(
            bpvec(
                X, FUN, ..., AGGREGATE=AGGREGATE, BPREDO=BPREDO,
                BPPARAM = param, BPOPTIONS = BPOPTIONS
            )
        )
    }

    si <- .splitX(seq_along(X), bpnworkers(BPPARAM), bptasks(BPPARAM))
    otasks <- bptasks(BPPARAM)
    bptasks(BPPARAM) <- 0L
    on.exit(bptasks(BPPARAM) <- otasks)

    FUN1 <- function(i, ...) FUN(X[i], ...)
    res <- bptry(bplapply(
        si, FUN1, ..., BPREDO=BPREDO, BPPARAM=BPPARAM, BPOPTIONS = BPOPTIONS
    ))

    if (is(res, "error") || !all(bpok(res)))
        stop(.error_bplist(res))

    if (any(lengths(res) != lengths(si)))
        stop(.error("length(FUN(X)) not equal to length(X)", "bpvec_error"))

    do.call(AGGREGATE, res)
})

setMethod("bpvec", c("ANY", "missing"),
    function(X, FUN, ..., AGGREGATE=c, BPREDO=list(),
             BPPARAM=bpparam(), BPOPTIONS = bpoptions())
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)
    bpvec(X, FUN, ..., AGGREGATE=AGGREGATE, BPREDO=BPREDO,
          BPPARAM=BPPARAM, BPOPTIONS = BPOPTIONS)
})

setMethod("bpvec", c("ANY", "list"),
    function(X, FUN, ..., BPREDO=list(),
             BPPARAM=bpparam(), BPOPTIONS = bpoptions())
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

    bpvec(
        X, myFUN, ..., BPREDO=BPREDO,
        BPPARAM=BPPARAM[[1]], BPOPTIONS = BPOPTIONS
    )
})
