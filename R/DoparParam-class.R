### =========================================================================
### DoparParam objects
### -------------------------------------------------------------------------


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

.DoparParam_prototype <- .BiocParallelParam_prototype

.DoparParam <- setRefClass("DoparParam",
    contains="BiocParallelParam",
    fields=list(),
    methods=list()
)

DoparParam <-
    function(stop.on.error=TRUE)
{
    if (!"package:foreach" %in% search()) {
        tryCatch({
            loadNamespace("foreach")
        }, error=function(err) {
            stop(conditionMessage(err), "\n",
                 "  DoparParam() requires the 'foreach' package")
        })
    }

    prototype <- .prototype_update(
        .DoparParam_prototype,
        stop.on.error=stop.on.error
    )

    x <- do.call(.DoparParam, prototype)
    validObject(x)
    x
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
        return(.rename(list(), X))

    FUN <- match.fun(FUN)
    BPREDO <- bpresult(BPREDO)

    idx <- .redo_index(X, BPREDO)
    if (length(idx))
        X <- X[idx]

    OPTIONS <- .workerOptions(
        log = bplog(BPPARAM),
        stop.on.error = bpstopOnError(BPPARAM),
        timeout = bptimeout(BPPARAM),
        exportglobals = bpexportglobals(BPPARAM)
    )
    FUN <- .composeTry(
        FUN, OPTIONS = OPTIONS, SEED = NULL
    )

    i <- NULL
    handle <- ifelse(bpstopOnError(BPPARAM), "stop", "pass")
    `%dopar%` <- foreach::`%dopar%`
    res <- tryCatch({
        foreach::foreach(X=X, .errorhandling=handle) %dopar% FUN(X, ...)
    }, error=function(e) {
        msg <- conditionMessage(e)
        pattern <- "task ([[:digit:]]+).*"
        if (!grepl(pattern, msg))
            stop(
                "'DoparParam()' foreach() error occurred: ", msg,
                call. = FALSE
            )
        txt <- "'DoparParam()' does not support partial results"
        updt <- rep(list(.error_not_available(txt)), length(X))
        i <- sub(pattern, "\\1", msg)
        updt[[as.integer(i)]] <- .error(msg)
        updt
    })

    names(res) <- names(X)

    if (length(BPREDO) && length(idx)) {
        BPREDO[idx] <- res
        res <- BPREDO
    }

    if (!.bpallok(res))
        stop(.error_bplist(res))

    res
})

setMethod("bpiterate", c("ANY", "ANY", "DoparParam"),
    function(ITER, FUN, ..., BPREDO = list(), BPPARAM=bpparam())
{
    stop("'bpiterate' not supported for DoparParam")
})
