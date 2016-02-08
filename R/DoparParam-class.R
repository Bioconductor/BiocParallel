### =========================================================================
### DoparParam objects
### -------------------------------------------------------------------------


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor 
###

.DoparParam <- setRefClass("DoparParam",
    contains="BiocParallelParam",
    fields=list(),
    methods=list()
)

DoparParam <-
    function(catch.errors=TRUE, stop.on.error=TRUE)
{
    if (!missing(catch.errors))
        warning("'catch.errors' is deprecated, use 'stop.on.error'")

    if (!"package:foreach" %in% search()) {
        tryCatch({
            loadNamespace("foreach")
        }, error=function(err) {
            stop(conditionMessage(err), "\n",
                 "  DoparParam() requires the 'foreach' package")
        })
    }
    .DoparParam(catch.errors=catch.errors, stop.on.error=stop.on.error)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - control
###

setMethod("bpworkers", "DoparParam",
    function(x)
{
    if (bpisup(x))
        foreach::getDoParWorkers()
    else 0L
})

setMethod("bpisup", "DoparParam",
    function(x)
{
    isNamespaceLoaded("foreach") && foreach::getDoParRegistered() &&
        (foreach::getDoParName() != "doSEQ") &&
        (foreach::getDoParWorkers() > 1L)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - evaluation
###

setMethod("bplapply", c("ANY", "DoparParam"),
    function(X, FUN, ..., BPREDO=list(), BPPARAM=bpparam())
{
    if (!length(X))
        return(list())

    FUN <- match.fun(FUN)

    idx <- .redo_index(X, BPREDO)
    if (any(idx))
        X <- X[idx]

    FUN <- .composeTry(FUN, bplog(BPPARAM), bpstopOnError(BPPARAM),
                       timeout=bptimeout(BPPARAM))

    i <- NULL
    handle <- ifelse(bpstopOnError(BPPARAM), "stop", "pass")
    `%dopar%` <- foreach::`%dopar%`
    res <- tryCatch({
        foreach::foreach(X=X, .errorhandling=handle) %dopar% FUN(X, ...)
    }, error=function(e) {
        txt <- "'DoparParam()' does not support partial results"
        updt <- rep(list(.error_not_available(txt)), length(X))
        msg <- conditionMessage(e)
        i <- sub("task ([[:digit:]]+).*", "\\1", msg)
        updt[[as.integer(i)]] <- .error(msg)
        updt
    })

    names(res) <- names(X)

    if (any(idx)) {
        BPREDO[idx] <- res
        res <- BPREDO 
    }

    if (!all(bpok(res)))
        stop(.error_bplist(res))

    res
})

setMethod("bpiterate", c("ANY", "ANY", "DoparParam"),
    function(ITER, FUN, ..., BPPARAM=bpparam())
{
    stop("'bpiterate' not supported for DoparParam")
})
