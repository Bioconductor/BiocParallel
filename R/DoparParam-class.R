.DoparParam <- setRefClass("DoparParam",
    contains="BiocParallelParam",
    fields=list()
)

DoparParam <-
    function(catch.errors=TRUE)
{
    .DoparParam(catch.errors=catch.errors)
}

## control

setMethod(bpworkers, "DoparParam",
    function(x, ...)
{
    if (bpisup(x))
        getDoParWorkers()
    else 0L
})

setMethod(bpisup, "DoparParam",
    function(x, ...)
{
    ("package:foreach" %in% search()) &&
        getDoParRegistered() && (getDoParName() != "doSEQ") &&
            getDoParWorkers() > 1L
})

## evaluation

setMethod(bpmapply, c("ANY", "DoparParam"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
        resume=getOption("BiocParallel.resume", FALSE), BPPARAM)
{
    FUN <- match.fun(FUN)
    if (resume) {
        results <- .resume(FUN=FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
            USE.NAMES=USE.NAMES, BPPARAM=BPPARAM)
        return(results)
    }
    if (!bpisup(BPPARAM)) {
        results <- bpmapply(FUN=FUN, ..., MoreArgs=MoreArgs,
            SIMPLIFY=SIMPLIFY, USE.NAMES=USE.NAMES, resume=resume,
            BPPARAM=SerialParam(catch.errors=BPPARAM$catch.errors))
        return(results)
    }

    ddd <- .getDotsForMapply(...)
    if (!is.list(MoreArgs))
        MoreArgs <- as.list(MoreArgs)

    if (BPPARAM$catch.errors)
        FUN <- .composeTry(FUN)

    i <- NULL
    results <-
      foreach(i=seq_len(length(ddd[[1L]])), .errorhandling="stop") %dopar% {
          do.call(FUN, args=c(lapply(ddd, "[[", i), MoreArgs))
    }
    results <- .rename(results, ddd, USE.NAMES=USE.NAMES)

    is.error <- vapply(results, inherits, logical(1L), what="remote-error")
    if (any(is.error))
        LastError$store(results=results, is.error=is.error, throw.error=TRUE)

    .simplify(results, SIMPLIFY=SIMPLIFY)
})
