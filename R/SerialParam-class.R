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

setMethod(bpmapply, c("ANY", "SerialParam"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
        resume=getOption("BiocParallel.resume", FALSE), BPPARAM)
{
    FUN <- match.fun(FUN)
    if (resume) {
        results <- .resume(FUN=FUN, ..., MoreArgs=MoreArgs,
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

        .simplify(results, SIMPLIFY=SIMPLIFY)
    }

    mapply(FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY, USE.NAMES=USE.NAMES)
})
