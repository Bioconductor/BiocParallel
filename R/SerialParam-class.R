### =========================================================================
### SerialParam objects
### -------------------------------------------------------------------------

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor 
###

.SerialParam_prototype <- c(
    list(
        workers = 1L,
        threshold="INFO",
        logdir=NA_character_
    ),
    .BiocParallelParam_prototype
)

.SerialParam <- setRefClass("SerialParam",
    contains="BiocParallelParam",
    fields=list(
        logdir="character"
    ),
    methods=list(
        show = function() {
            callSuper()
            cat("  bplogdir: ", bplogdir(.self), "\n", sep="")
        })
)

SerialParam <-
    function(stop.on.error = TRUE,
             log=FALSE, threshold="INFO", logdir=NA_character_,
             progressbar=FALSE)
{
    prototype <- .prototype_update(
        .SerialParam_prototype,
        stop.on.error=stop.on.error,
        log=log, threshold=threshold, logdir=logdir,
        progressbar=progressbar
    )
    x <- do.call(.SerialParam, prototype)
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

    FUN <- .composeTry(
        FUN, bplog(BPPARAM), bpstopOnError(BPPARAM), bpstopOnError(BPPARAM),
        timeout=bptimeout(BPPARAM), exportglobals=FALSE
    )

    progress <- .progress(active=bpprogressbar(BPPARAM))
    on.exit(progress$term(), TRUE)
    progress$init(length(X))
    FUN_ <- function(...) {
        value <- FUN(...)
        progress$step()
        value
    }
    res <- lapply(X, FUN_, ...)

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
    reducer <- .reducer(REDUCE, init)
    i <- 0L
    repeat {
        value <- ITER()
        if(is.null(value))
            break
        i <- i + 1L
        value <- FUN(value, ...)
        reducer$add(i, value)
    }
    reducer$value()
}

setMethod("bpiterate", c("ANY", "ANY", "SerialParam"),
    function(ITER, FUN, ..., REDUCE, init, reduce.in.order = FALSE,
        BPPARAM=bpparam())
{
    ITER <- match.fun(ITER)
    FUN <- match.fun(FUN)

    .log_load(bplog(BPPARAM), bpthreshold(BPPARAM))

    FUN <- .composeTry(
        FUN, bplog(BPPARAM), bpstopOnError(BPPARAM),
        timeout=bptimeout(BPPARAM), exportglobals=FALSE
    )
    progress <- .progress(active=bpprogressbar(BPPARAM), iterate=TRUE)
    on.exit(progress$term(), TRUE)
    progress$init()
    FUN_ <- function(...) {
        value <- FUN(...)
        progress$step()
        value
    }

    .bpiterate_serial(ITER, FUN_, ..., REDUCE = REDUCE, init = init)
})
