### =========================================================================
### bpvec methods 
### -------------------------------------------------------------------------

## MulticoreParam has a dedicated bpvec() method all others use
## bpvec,ANY,BiocParallelParam. bpvec() dispatches to bplapply()
## where errors and logging are handled.

setMethod(bpvec, c("ANY", "missing"),
    function(X, FUN, ..., AGGREGATE=c, BPREDO=list(), BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)
    bpvec(X, FUN, ..., AGGREGATE=AGGREGATE, BPREDO=BPREDO, BPPARAM=BPPARAM)
})

setMethod(bpvec, c("ANY", "BiocParallelParam"),
    function(X, FUN, ..., AGGREGATE=c, BPREDO=list(), BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)

    if (is.na(bpworkers(BPPARAM)))
        stop("'bpworkers' must be set in your backend to use bpvec")
    ## pvec parellelizes execution of a function on vector elements 
    ## by splitting the vector and submitting each part to one core.
    ## This is equivalent to saying length(X) is evenly divided over
    ## available cores which is the (default) behavior of bplapply()
    ## for SnowParam and MulticoreParam when tasks = 0.
    if (is(BPPARAM, "DoparParam") || is(BPPARAM, "BatchJobsParam"))
        X <- .splitX(X, bpworkers(BPPARAM), 0)
    else if (bptasks(BPPARAM) != 0)
        bptasks(BPPARAM) <- 0
    ans <- bplapply(X, FUN, ..., BPREDO=BPREDO, BPPARAM=BPPARAM) 
    do.call(AGGREGATE, ans)
})

