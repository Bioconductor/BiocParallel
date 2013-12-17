.SerialParam <- setRefClass("SerialParam",
    contains="BiocParallelParam",
    fields=list()
)

SerialParam <-
    function(catch.errors=TRUE)
{
    .SerialParam(catch.errors=catch.errors, workers=1L)
}

## control

setMethod(bpworkers, "SerialParam", function(x, ...) 1L)

setMethod(bpisup, "SerialParam", function(x, ...) TRUE)

setMethod(bplapply, c("ANY", "SerialParam"),
    function(X, FUN, ...,
        BPRESUME=getOption("BiocParallel.BPRESUME", FALSE), BPPARAM)
{
    FUN <- match.fun(FUN)
    if (BPRESUME) {
        return(.bpresume_lapply(X=X, FUN=FUN, ..., BPPARAM=BPPARAM))
    }

    if (BPPARAM$catch.errors) {
        FUN <- .composeTry(FUN)
        results <- lapply(X, FUN, ...)

        is.error <- vapply(results, inherits, logical(1L), what="remote-error")
        if (any(is.error))
            LastError$store(results=results, is.error=is.error,
                throw.error=TRUE)
        return(results)
    }

    lapply(X, FUN, ...)
})

setMethod(bpmapply, c("ANY", "SerialParam"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
        BPRESUME=getOption("BiocParallel.BPRESUME", FALSE), BPPARAM)
{
    FUN <- match.fun(FUN)
    if (BPRESUME) {
        results <- .bpresume_mapply(FUN=FUN, ..., MoreArgs=MoreArgs,
            SIMPLIFY=SIMPLIFY, USE.NAMES=USE.NAMES, BPPARAM=BPPARAM)
        return(results)
    }

    if (BPPARAM$catch.errors) {
        FUN <- .composeTry(FUN)
        results <- mapply(FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=FALSE,
            USE.NAMES=USE.NAMES)

        is.error <- vapply(results, inherits, logical(1L),
                           what="remote-error")
        if (any(is.error))
            LastError$store(results=results, is.error=is.error,
                            throw.error=TRUE)

        return(.simplify(results, SIMPLIFY=SIMPLIFY))
    }

    mapply(FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY, USE.NAMES=USE.NAMES)
})
