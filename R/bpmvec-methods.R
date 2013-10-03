## TODO: Use bpaggregate?
setMethod(bpmvec, c("ANY", "ANY"),
    function(FUN, ..., MoreArgs=NULL, AGGREGATE=c,  BPPARAM)
{
    FUN <- match.fun(FUN)
    all.args <- c(list(...), MoreArgs)
    do.call(FUN, all.args)
})

## TODO: Resume support?
## Default method that just uses bplapply internally
setMethod(bpmvec, c("ANY", "BiocParallelParam"),
    function(FUN, ..., MoreArgs=NULL, AGGREGATE=c,  BPPARAM)
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)

    ddd = .getDotsForMapply(...)
    if (!is.list(MoreArgs))
      MoreArgs = as.list(MoreArgs)
    tasks <- length(ddd[[1L]])
    workers <- min(tasks, bpworkers(BPPARAM))
    ## TODO: have a workers arg or chunksize arg?
    if (is.na(workers))
      stop("'n.workers' must be set in your backend to use bpmvec")

    si <- .splitIndices(tasks, workers)
    ans <- bplapply(si, function(i, vargs, sargs, FUN) {
        args <- c(lapply(vargs, `[`, i), sargs)
        do.call(FUN, args)
    }, vector.args, MoreArgs, BPPARAM=BPPARAM)
    do.call(AGGREGATE, ans)
})

setMethod(bpmvec, c("ANY", "missing"),
    function(FUN, ..., MoreArgs=NULL, AGGREGATE=c,  BPPARAM)
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)

    x <- registered()[[1]]
    bpmvec(X, FUN, ..., AGGREGATE=AGGREGATE, BPPARAM=x)
})
