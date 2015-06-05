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
            cat("  bpworkers:", bpworkers(.self),
                   "; bpisup:", bpisup(.self), "\n", sep="")
        })
)

DoparParam <-
    function(catch.errors=TRUE)
{
    if (!catch.errors)
        warning("'catch.errors' has been deprecated")
    if (!"package:foreach" %in% search()) {
        tryCatch({
            attachNamespace("foreach")
        }, error=function(err) {
            stop(conditionMessage(err), 
                ": DoparParam class objects require the 'foreach' package")
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
            stop("no error detected in 'BPREDO'")
        if (length(BPREDO) != length(X))
            stop("length(BPREDO) must equal length(X)")
        message("Resuming previous calculation ... ")
        X <- X[idx]
    }
    nms <- names(X)

    if (!bpisup(BPPARAM))
        return(bplapply(X, FUN=FUN, ..., BPPARAM=SerialParam()))
    if (bpcatchErrors(BPPARAM))
        FUN <- .composeTry(FUN)

    i <- NULL
    handle <- ifelse(bpcatchErrors(BPPARAM), "pass", "stop")
    res <- foreach(i=seq_along(X), .errorhandling=handle) %dopar% 
        { FUN(X[[i]], ...) }

    if (!is.null(res)) {
        names(res) <- nms
    }
    if (length(BPREDO)) {
        BPREDO[idx] <- res
        BPREDO 
    } else res
})

setMethod(bpiterate, c("ANY", "ANY", "DoparParam"),
    function(ITER, FUN, ..., BPPARAM=bpparam())
{
    stop(paste0("bpiterate not supported for DoparParam"))
})
