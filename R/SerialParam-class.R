### =========================================================================
### SerialParam objects
### -------------------------------------------------------------------------

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor 
###

.SerialParam <- setRefClass("SerialParam",
    contains="BiocParallelParam",
    fields=list()
)

SerialParam <-
    function()
{
    .SerialParam(workers=1L)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - control
###

setMethod(bpworkers, "SerialParam", function(x, ...) 1L)

setMethod(bpisup, "SerialParam", function(x, ...) TRUE)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - evaluation
###

setMethod(bplapply, c("ANY", "SerialParam"),
    function(X, FUN, ..., BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    lapply(X, FUN, ...)
})

.bpiterate_serial <- function(ITER, FUN, ..., REDUCE, init)
{
    ITER <- match.fun(ITER)
    FUN <- match.fun(FUN)
    N_GROW <- 100L
    n <- 0
    result <- vector("list", n)
    if (!missing(init)) result[[1]] <- init
    i <- 0L
    repeat {
        if(is.null(dat <- ITER()))
            break
        else
            value <- FUN(dat, ...)

        if (missing(REDUCE)) {
            i <- i + 1L
            if (i > n) {
                n <- n + N_GROW
                length(result) <- n
            }
            result[[i]] <- value
        } else {
            if (length(result))
                result[[1]] <- REDUCE(result[[1]], unlist(value))
            else
                result[[1]] <- value 
        }
    }
    length(result) <- ifelse(i == 0L, 1, i)
    result
}

setMethod(bpiterate, c("ANY", "ANY", "SerialParam"),
    function(ITER, FUN, ..., BPPARAM=bpparam())
{
    .bpiterate_serial(ITER, FUN, ...)
})
