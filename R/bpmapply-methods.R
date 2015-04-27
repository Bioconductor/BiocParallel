### =========================================================================
### bpmapply methods 
### -------------------------------------------------------------------------

## BatchJobsParam has a dedicated bpmapply() method. All others dispatch
## to bplapply() where errors and logging are handled.


setMethod(bpmapply, c("ANY", "missing"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, 
             USE.NAMES=TRUE, BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    bpmapply(FUN, ..., BPPARAM=BPPARAM)
})

setMethod(bpmapply, c("ANY", "BiocParallelParam"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, 
             USE.NAMES=TRUE, BPPARAM=bpparam())
{
    ## re-package for lapply
    ddd <- .getDotsForMapply(...)
    if (!length(ddd) || !length(ddd[[1L]]))
      return(list())

    FUN <- match.fun(FUN)
    wrap <- function(.i, .FUN, .ddd, .MoreArgs) {
        dots <- lapply(.ddd, `[`, .i)
        .mapply(.FUN, dots, .MoreArgs)[[1L]]
    }

    res <- bplapply(X=seq_along(ddd[[1L]]), wrap, BPPARAM=BPPARAM, 
                    .FUN=FUN, .ddd=ddd, .MoreArgs=MoreArgs)
    .simplify(.rename(res, ddd, USE.NAMES=USE.NAMES), SIMPLIFY)
})

setMethod(bpmapply, c("ANY", "list"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, 
             USE.NAMES=TRUE, BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)

    if (!all(vapply(BPPARAM, is, logical(1), "BiocParallelParam")))
        stop("All elements in 'BPPARAM' must be BiocParallelParam objects")
    if (length(BPPARAM) == 0L)
        stop("'length(BPPARAM)' must be > 0")

    myFUN <- 
        if (length(BPPARAM) > 1L)
            function(...) FUN(..., BPPARAM=BPPARAM[-1L])
        else FUN
    bpmapply(myFUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
             USE.NAMES=TRUE, BPPARAM=BPPARAM[[1L]])
})
