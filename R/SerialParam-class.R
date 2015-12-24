### =========================================================================
### SerialParam objects
### -------------------------------------------------------------------------

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor 
###

.SerialParam <- setRefClass("SerialParam",
    contains="BiocParallelParam",
    fields=list(
        logdir="character"),
    methods=list(
        initialize = function(...,
            threshold="INFO",
            logdir=NA_character_)
        { 
            callSuper(...)
            initFields(threshold=threshold, logdir=logdir)
        },
        show = function() {
            callSuper()
            cat("\n  bplogdir: ", bplogdir(.self), "\n", sep="")
        })
)

SerialParam <-
    function(catch.errors=TRUE, stop.on.error = TRUE,
             log=FALSE, threshold="INFO", logdir=NA_character_)
{
    if (!missing(catch.errors))
        warning("'catch.errors' is deprecated, use 'stop.on.error'")

    x <- .SerialParam(workers=1L, catch.errors=catch.errors,
                      stop.on.error=stop.on.error,
                      log=log, threshold=threshold, logdir=logdir) 
    validObject(x)
    x
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - control
###

setMethod("bpworkers", "SerialParam", function(x) 1L)

setMethod("bpisup", "SerialParam", function(x) TRUE)

setReplaceMethod("bplog", c("SerialParam", "logical"),
    function(x, value)
{
    x$log <- value 
    validObject(x)
    x
})

setReplaceMethod("bpthreshold", c("SerialParam", "character"),
    function(x, value)
{
    x$threshold <- value
    x
})

setMethod("bplogdir", "SerialParam",
    function(x)
{
    x$logdir
})

setReplaceMethod("bplogdir", c("SerialParam", "character"),
    function(x, value)
{
    if (!length(value))
        value <- NA_character_
    x$logdir <- value 
    if (is.null(msg <- .valid.SnowParam.log(x))) 
        x
    else 
        stop(msg)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - evaluation
###

setMethod("bplapply", c("ANY", "SerialParam"),
    function(X, FUN, ..., BPREDO=list(), BPPARAM=bpparam())
{
    if (!length(X))
        return(list())

    FUN <- match.fun(FUN)

    idx <- .redo_index(X, BPREDO)
    if (any(idx))
        X <- X[idx]

    .log_load(bplog(BPPARAM), bpthreshold(BPPARAM))

    FUN <- .composeTry(FUN, bplog(BPPARAM), bpstopOnError(BPPARAM),
                       stop.immediate=bpstopOnError(BPPARAM),
                       timeout=bptimeout(BPPARAM))

    res <- lapply(X, FUN, ...)

    names(res) <- names(X)

    if (any(idx)) {
        BPREDO[idx] <- res
        res <- BPREDO 
    }

    if (!all(bpok(res)))
        stop(.error_bplist(res))

    res
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

setMethod("bpiterate", c("ANY", "ANY", "SerialParam"),
    function(ITER, FUN, ..., BPPARAM=bpparam())
{
    ITER <- match.fun(ITER)
    FUN <- match.fun(FUN)

    .log_load(bplog(BPPARAM), bpthreshold(BPPARAM))

    FUN <- .composeTry(FUN, bplog(BPPARAM), bpstopOnError(BPPARAM),
                       timeout=bptimeout(BPPARAM))

    .bpiterate_serial(ITER, FUN, ...)
})
