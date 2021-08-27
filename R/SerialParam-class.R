### =========================================================================
### SerialParam objects
### -------------------------------------------------------------------------

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

.SerialParam_prototype <- c(
    list(
        workers = list(FALSE)
    ),
    .BiocParallelParam_prototype
)

.SerialParam <- setRefClass(
    "SerialParam",
    contains="BiocParallelParam",
)

SerialParam <-
    function(stop.on.error = TRUE,
             log=FALSE, threshold="INFO", logdir=NA_character_,
             progressbar=FALSE, RNGseed = NULL)
{
    if (!is.null(RNGseed))
        RNGseed <- as.integer(RNGseed)

    prototype <- .prototype_update(
        .SerialParam_prototype,
        stop.on.error=stop.on.error,
        log=log, threshold=threshold, logdir=logdir,
        progressbar=progressbar, RNGseed = RNGseed
    )
    x <- do.call(.SerialParam, prototype)
    validObject(x)
    x
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - control
###

setMethod(
    "bpbackend", "SerialParam",
    function(x)
{
    bpworkers(x)
})

setMethod(
    "bpstart", "SerialParam",
    function(x, ...)
{
    x$workers <- list(TRUE)
    .bpstart_impl(x)
})

setMethod(
    "bpstop", "SerialParam",
    function(x)
{
    x$workers <- list(FALSE)
    .bpstop_impl(x)
})

setMethod(
    "bpisup", "SerialParam",
    function(x)
{
    identical(bpbackend(x), list(TRUE))
})

setReplaceMethod("bplog", c("SerialParam", "logical"),
    function(x, value)
{
    x$log <- value
    validObject(x)
    x
})

setReplaceMethod(
    "bpthreshold", c("SerialParam", "character"),
    function(x, value)
{
    x$threshold <- value
    x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - evaluation
###

setMethod("bplapply", c("ANY", "SerialParam"),
    function(X, FUN, ..., BPREDO=list(), BPPARAM=bpparam())
{
    if (!length(X))
        return(.rename(list(), X))

    FUN <- match.fun(FUN)

    redo_index <- .redo_index(X, BPREDO)
    if (any(redo_index)) {
        X <- X[redo_index]
        compute_element <- redo_index
    } else {
        compute_element <- rep(TRUE, length(X))
    }

    if (!bpisup(BPPARAM)) {
        bpstart(BPPARAM)
        on.exit(bpstop(BPPARAM), TRUE)
    }

    .log_load(bplog(BPPARAM), bpthreshold(BPPARAM))

    FUN <- .composeTry(
        FUN, bplog(BPPARAM), bpstopOnError(BPPARAM), bpstopOnError(BPPARAM),
        timeout=bptimeout(BPPARAM), exportglobals=FALSE
    )

    progress <- .progress(active=bpprogressbar(BPPARAM))
    on.exit(progress$term(), TRUE)
    progress$init(length(X))
    FUN_ <- function(...) {
        value <- tryCatch(FUN(...), error = identity)
        progress$step()
        value
    }
    BPRNGSEED <- .rng_seeds_by_task(BPPARAM, compute_element, length(X))[[1]]
    res <- .rng_lapply(X, FUN_, ..., BPRNGSEED = BPRNGSEED)

    names(res) <- names(X)

    if (any(redo_index)) {
        BPREDO[redo_index] <- res
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

    if (!bpisup(BPPARAM)) {
        bpstart(BPPARAM)
        on.exit(bpstop(BPPARAM), TRUE)
    }
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

    BPRNGSEED <- .rng_seeds_by_task(BPPARAM, TRUE, 1L)[[1]]
    FUN__ <- .rng_job_fun_factory(FUN_, BPRNGSEED)

    .bpiterate_serial(ITER, FUN__, ..., REDUCE = REDUCE, init = init)
})
