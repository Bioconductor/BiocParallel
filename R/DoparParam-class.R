.DoparParamSingleton <-
    setRefClass("DoparParam",
                contains="BiocParallelParam")()

DoparParam <- function() .DoparParamSingleton

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
    function(X, FUN, ..., BPPARAM)
{
    FUN <- match.fun(FUN)
    ## If no parallel backend is registered for foreach, fall back to
    ## the serial backend.
    if (!bpisup(BPPARAM))
        return(bplapply(X, FUN, ..., BPPARAM=SerialParam()))

    x <- NULL                           # quieten R CMD check
    ans <- foreach(x=X) %dopar% FUN(x, ...)
    setNames(ans, names(X))
})
