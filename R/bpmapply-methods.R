setMethod(bpmapply, c("ANY", "missing"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
             BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    bpmapply(FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
             USE.NAMES=USE.NAMES, BPPARAM=BPPARAM)
})

## BatchJobsParam has method for bpmapply, all others dispatch to bplapply.
setMethod(bpmapply, c("ANY", "BiocParallelParam"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
        BPPARAM=bpparam())
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
