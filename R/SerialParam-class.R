### =========================================================================
### SerialParam objects
### -------------------------------------------------------------------------

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor 
###

.SerialParam <- setRefClass("SerialParam",
    contains="BiocParallelParam",
    fields=list(
        log="logical",
        threshold="ANY"),
    methods=list(
        initialize = function(..., 
            log=FALSE,
            threshold="INFO")
        { 
            initFields(log=log, threshold=threshold)
            callSuper(...)
        },
        show = function() {
            callSuper()
            cat("  bpstopOnError:", bpstopOnError(.self), "\n", sep="")
            cat("  bplog:", bplog(.self),
                   "; bpthreshold:", names(bpthreshold(.self)), "\n", sep="")
        })
)

SerialParam <- function(catch.errors=TRUE, stop.on.error=FALSE, 
                        log=FALSE, threshold="INFO")
{
    if (!catch.errors)
        warning("'catch.errors' has been deprecated")
    x <- .SerialParam(workers=1L, 
                      catch.errors=catch.errors, stop.on.error=stop.on.error,
                      log=log, threshold=.THRESHOLD(threshold)) 
    validObject(x)
    x
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - control
###

setMethod(bpworkers, "SerialParam", function(x, ...) 1L)

setMethod(bpisup, "SerialParam", function(x, ...) TRUE)

setMethod("bplog", "SerialParam",
    function(x, ...)
{
    x$log
})

setReplaceMethod("bplog", c("SerialParam", "logical"),
    function(x, ..., value)
{
    x$log <- value 
    validObject(x)
    x
})

setMethod("bpthreshold", "SerialParam",
    function(x, ...)
{
    x$threshold
})

setReplaceMethod("bpthreshold", c("SerialParam", "character"),
    function(x, ..., value)
{
    nms <- c("TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL")
    if (!value %in% nms)
        stop(paste0("'value' must be one of ",
             paste(sQuote(nms), collapse=", ")))
    x$threshold <- .THRESHOLD(value) 
    x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - evaluation
###

setMethod(bplapply, c("ANY", "SerialParam"),
    function(X, FUN, ..., BPREDO=list(), BPPARAM=bpparam())
{
    if (!length(X))
        return(list())
    FUN <- match.fun(FUN)
    if (length(BPREDO)) {
        if (all(idx <- !bpok(BPREDO)))
            stop("no error detected in 'BPREDO'")
        if (length(BPREDO) != length(X))
            stop("length(BPREDO) must equal length(X)")
        message("Resuming previous calculation ... ")
        X <- X[idx]
    }

    ## logging
    if (bplog(BPPARAM)) {
        if (!"package:futile.logger" %in% search()) {
            tryCatch({
                attachNamespace("futile.logger")
            }, error=function(err) {
                msg <- "logging requires the 'futile.logger' package"
                stop(conditionMessage(err), msg) 
            })
        }
        flog.info("loading futile.logger package")
        flog.threshold(bpthreshold(BPPARAM))
    } else FUN <- .composeTry(FUN, FALSE)

    ## error handling
    if (!bpstopOnError(BPPARAM)) {
        if (bplog(BPPARAM))
            FUN <- .composeTry(FUN, TRUE)
        else
            FUN <- .composeTry(FUN, FALSE)
    }

    res <- lapply(X, FUN, ...)
    if (length(BPREDO)) {
        BPREDO[idx] <- res
        BPREDO 
    } else res
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
    ITER <- match.fun(ITER)
    FUN <- match.fun(FUN)
    if (bplog(BPPARAM) || bpstopOnError(BPPARAM))
        FUN <- .composeTry(FUN, TRUE)
    else
        FUN <- .composeTry(FUN, FALSE)

    .bpiterate_serial(ITER, FUN, ...)
})
