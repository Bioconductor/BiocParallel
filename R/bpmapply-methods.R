### =========================================================================
### bpmapply methods
### -------------------------------------------------------------------------

## bpmapply() dispatches to bplapply() where errors and logging are handled.

setMethod("bpmapply", c("ANY", "BiocParallelParam"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE,
             USE.NAMES=TRUE, BPREDO=list(),
             BPPARAM=bpparam(), BPOPTIONS = bpoptions())
{
    ## re-package for lapply
    ddd <- .getDotsForMapply(...)
    if (!length(ddd) || !length(ddd[[1L]]))
        return(.mrename(list(), ddd, USE.NAMES))

    FUN <- match.fun(FUN)
    wrap <- function(.i, .FUN, .ddd, .MoreArgs) {
        dots <- lapply(.ddd, `[`, .i)
        .mapply(.FUN, dots, .MoreArgs)[[1L]]
    }

    res <- bplapply(X=seq_along(ddd[[1L]]), wrap, .FUN=FUN, .ddd=ddd,
                    .MoreArgs=MoreArgs, BPREDO=BPREDO,
                    BPPARAM=BPPARAM, BPOPTIONS = BPOPTIONS)
    .simplify(.mrename(res, ddd, USE.NAMES), SIMPLIFY)
})

setMethod("bpmapply", c("ANY", "missing"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE,
             USE.NAMES=TRUE, BPREDO=list(),
             BPPARAM=bpparam(), BPOPTIONS = bpoptions())
{
    FUN <- match.fun(FUN)
    bpmapply(FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
             USE.NAMES=USE.NAMES, BPREDO=BPREDO,
             BPPARAM=BPPARAM, BPOPTIONS = BPOPTIONS)
})

setMethod("bpmapply", c("ANY", "list"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE,
             USE.NAMES=TRUE, BPREDO=list(),
             BPPARAM=bpparam(), BPOPTIONS = bpoptions())
{
    FUN <- match.fun(FUN)

    if (!all(vapply(BPPARAM, inherits, logical(1), "BiocParallelParam")))
        stop("All elements in 'BPPARAM' must be BiocParallelParam objects")
    if (length(BPPARAM) == 0L)
        stop("'length(BPPARAM)' must be > 0")

    myFUN <-
        if (length(BPPARAM) > 1L) {
          if (length(param <- BPPARAM[-1]) == 1L)
            function(...) FUN(..., BPPARAM=param[[1]])
          else
            function(...) FUN(..., BPPARAM=param)
        } else FUN
    bpmapply(myFUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
             USE.NAMES=USE.NAMES, BPREDO=BPREDO,
             BPPARAM=BPPARAM[[1L]], BPOPTIONS = BPOPTIONS)
})
