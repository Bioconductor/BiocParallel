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
        })
)

DoparParam <-
    function(catch.errors=TRUE)
{
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

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - evaluation
###

setMethod(bplapply, c("ANY", "DoparParam"),
    function(X, FUN, ..., BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)

    if (!bpisup(BPPARAM))
        return(bplapply(X, FUN=FUN, ..., BPPARAM=SerialParam()))
    if (bpcatchErrors(BPPARAM))
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

setMethod(bpiterate, c("ANY", "ANY", "DoparParam"),
    function(ITER, FUN, ..., BPPARAM=bpparam())
{
    stop(paste0("bpiterate not supported for DoparParam"))
})
