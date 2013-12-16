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
setMethod(bplapply, c("ANY", "DoparParam"),
    function(X, FUN, ...,
        BPRESUME=getOption("BiocParallel.BPRESUME", FALSE), BPPARAM)
{
    FUN <- match.fun(FUN)
    if (BPRESUME) {
        return(.bpresume_lapply(FUN=FUN, ..., BPPARAM=BPPARAM))
    }
    if (!bpisup(BPPARAM)) {
        return(bplapply(FUN=FUN, ...,
            BPPARAM=SerialParam(catch.errors=BPPARAM$catch.errors)))
    }
    if (BPPARAM$catch.errors)
        FUN <- .composeTry(FUN)

    i <- NULL
    results <-
      foreach(i=seq_along(X), .errorhandling="stop") %dopar% {
          FUN(X[[i]], ...)
    }

    is.error <- vapply(results, inherits, logical(1L), what="remote-error")
    if (any(is.error))
        LastError$store(results=results, is.error=is.error, throw.error=TRUE)

    results
})

setMethod(bpmapply, c("ANY", "DoparParam"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
        BPRESUME=getOption("BiocParallel.BPRESUME", FALSE), BPPARAM)
{
    FUN <- match.fun(FUN)
    if (BPRESUME) {
        results <- .bpresume_mapply(FUN=FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
            USE.NAMES=USE.NAMES, BPPARAM=BPPARAM)
        return(results)
    }
    if (!bpisup(BPPARAM)) {
        results <- bpmapply(FUN=FUN, ..., MoreArgs=MoreArgs,
            SIMPLIFY=SIMPLIFY, USE.NAMES=USE.NAMES, BPRESUME=BPRESUME,
            BPPARAM=SerialParam(catch.errors=BPPARAM$catch.errors))
        return(results)
    }

    ddd <- .getDotsForMapply(...)
    if (!length(ddd) || !length(ddd[[1L]]))
      return(list())
    if (!is.list(MoreArgs))
        MoreArgs <- as.list(MoreArgs)

    if (BPPARAM$catch.errors)
        FUN <- .composeTry(FUN)

    i <- NULL
    results <-
      foreach(i=seq_along(ddd[[1L]]), .errorhandling="stop") %dopar% {
          dots <- lapply(ddd, `[`, i)
          .mapply(FUN, dots, MoreArgs)[[1L]]
      }
    results <- .rename(results, ddd, USE.NAMES=USE.NAMES)

    is.error <- vapply(results, inherits, logical(1L), what="remote-error")
    if (any(is.error))
        LastError$store(results=results, is.error=is.error, throw.error=TRUE)

    .simplify(results, SIMPLIFY=SIMPLIFY)
})
