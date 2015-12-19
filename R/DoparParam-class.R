### =========================================================================
### DoparParam objects
### -------------------------------------------------------------------------


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor 
###

.DoparParam <- setRefClass("DoparParam",
    contains="BiocParallelParam",
    fields=list(),
    methods=list(
        show = function() {
            callSuper()
            cat("  bpworkers: ", bpworkers(.self),
                "; bpisup: ", bpisup(.self),
                "\n", sep="")
        })
)

DoparParam <-
    function(catch.errors=TRUE)
{
    if (!missing(catch.errors))
        warning("'catch.errors' is deprecated, use 'stop.on.error'")

    if (!"package:foreach" %in% search()) {
        tryCatch({
            attachNamespace("foreach")
        }, error=function(err) {
            stop(conditionMessage(err), "\n",
                 "  DoparParam() requires the 'foreach' package")
        })
    }
    .DoparParam(catch.errors=catch.errors)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - control
###

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
    if ("package:foreach" %in% search() && getDoParRegistered() && 
        (getDoParName() != "doSEQ") && getDoParWorkers() > 1L) {
        TRUE
    } else {
        FALSE
    }
})

## never enable logging or timeout
setMethod(bplog, "DoparParam", function(x, ...) FALSE)

setReplaceMethod("bplog", c("DoparParam", "logical"), 
    function(x, ..., value)
{
    stop("'bplog(x) <- value' not supported for DoparParam")
})

setMethod(bptimeout, "DoparParam", function(x, ...) Inf)

setReplaceMethod("bptimeout", c("DoparParam", "numeric"),
    function(x, ..., value)
{
    stop("'bptimeout(x) <- value' not supported for DoparParam")
})

setReplaceMethod("bpstopOnError", c("DoparParam", "logical"),
    function(x, ..., value)
{
    if (value)
        stop("'stop.on.error == TRUE' not implemented for DoparParam")
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - evaluation
###

setMethod(bplapply, c("ANY", "DoparParam"),
    function(X, FUN, ..., BPREDO=list(), BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    if (length(BPREDO)) {
        if (all(idx <- !bpok(BPREDO)))
            stop("no previous error in 'BPREDO'")
        if (length(BPREDO) != length(X))
            stop("length(BPREDO) must equal length(X)")
        message("Resuming previous calculation ... ")
        X <- X[idx]
    }
    nms <- names(X)

    if (!bpisup(BPPARAM))
        return(bplapply(X, FUN=FUN, ..., BPPARAM=SerialParam()))

    FUN <- .composeTry(FUN, bplog(BPPARAM), bpstopOnError(BPPARAM),
                       timeout=bptimeout(BPPARAM))

    i <- NULL
    handle <- ifelse(bpcatchErrors(BPPARAM), "pass", "stop")
    res <- foreach(i=seq_along(X), .errorhandling=handle) %dopar% 
        { FUN(X[[i]], ...) }

    if (!is.null(res))
        names(res) <- nms

    if (length(BPREDO)) {
        BPREDO[idx] <- res
        res <- BPREDO 
    }

    if (!all(bpok(res)))
        stop(.error_bplist(res))

    res
})

setMethod(bpiterate, c("ANY", "ANY", "DoparParam"),
    function(ITER, FUN, ..., BPPARAM=bpparam())
{
    stop(paste0("bpiterate not supported for DoparParam"))
})
