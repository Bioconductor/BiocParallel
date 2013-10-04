## TODO: Use bpaggregate?
setMethod(bpmvec, c("ANY", "ANY"),
    function(FUN, ..., MoreArgs=NULL, AGGREGATE=c,  BPPARAM)
{
    bpmvec(FUN, ..., MoreArgs=MoreArgs, AGGREGATE=AGGREGATE, BPPARAM=SerialParam())
})

setMethod(bpmvec, c("ANY", "SerialParam"),
    function(FUN, ..., MoreArgs=NULL, AGGREGATE=c,  BPPARAM)
{
    FUN <- match.fun(FUN)
    if (!is.list(MoreArgs))
      MoreArgs = as.list(MoreArgs)
    all.args <- c(list(...), MoreArgs)
    do.call(FUN, all.args)
})

## TODO: Resume/error support?
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
    else if (workers <= 1)
        return(bpmvec(FUN, ..., MoreArgs=MoreArgs, AGGREGATE=AGGREGATE, BPPARAM=SerialParam()))

    si <- .splitIndices(tasks, workers)
    ans <- bplapply(si, function (i, vargs, sargs, thefunc) {
        args <- c(lapply(vargs, `[`, i), sargs)
        do.call(FUN, args)
    }, vargs=ddd, sargs=MoreArgs, thefunc=FUN, BPPARAM=BPPARAM)
    do.call(AGGREGATE, ans)
})

setMethod(bpmvec, c("ANY", "missing"),
    function(FUN, ..., MoreArgs=NULL, AGGREGATE=c,  BPPARAM)
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)

    x <- registered()[[1]]
    bpmvec(FUN, ..., MoreArgs=MoreArgs, AGGREGATE=AGGREGATE, BPPARAM=x)
})
