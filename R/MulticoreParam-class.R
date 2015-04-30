### =========================================================================
### MulticoreParam objects
### -------------------------------------------------------------------------

multicoreWorkers <- function() {
    cores <- if (.Platform$OS.type == "windows")
        1
    else
        min(8L, parallel::detectCores())
    getOption("mc.cores", cores)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor 
###

.MulticoreParam <- setRefClass("MulticoreParam",
    contains="SnowParam",
    fields=list(
        cluster="cluster",
        .clusterargs="list",
        .controlled="logical",
        log="logical",
        threshold="ANY",
        logdir="character",
        resultdir="character"),
    methods=list(
        initialize = function(..., 
            .controlled=TRUE,
            log=FALSE,
            threshold="INFO",
            logdir=character(),
            resultdir=character())
        { 
            callSuper(...)
            initFields(.controlled=.controlled, log=log, threshold=threshold, 
                       logdir=logdir, resultdir=resultdir)
        },
        show = function() {
            callSuper()
        })
)

MulticoreParam <- function(workers=multicoreWorkers(), 
        tasks=0L, catch.errors=TRUE, stop.on.error=FALSE, 
        log=FALSE, threshold="INFO", logdir=character(),
        resultdir=character(), ...)
{
    .MulticoreParam(workers=as.integer(workers), 
        tasks=as.integer(tasks), catch.errors=catch.errors, 
        stop.on.error=stop.on.error, 
        log=log, threshold=.THRESHOLD(threshold), logdir=logdir, 
        resultdir=resultdir,
        .clusterargs=list(spec=as.integer(workers), type="FORK"), ...)
}

setValidity("MulticoreParam",
    function(object)
{
    msg <- NULL
    txt <- function(fmt, flds)
        sprintf(fmt, paste(sQuote(flds), collapse=", "))

    fields <- names(.paramFields(.MulticoreParam))

    FUN <- function(i, x) length(x[[i]])
    isScalar <- sapply(fields, FUN, object) == 1L
    if (!all(isScalar))
        msg <- c(msg, txt("%s must be length 1", fields[!isScalar]))

    FUN <- function(i, x) is.na(x[[i]])
    isNA <- sapply(fields[isScalar], FUN, object)
    if (any(isNA))
        msg <- c(msg, txt("%s must be length 1", fields[isNA]))

    if (!is.null(msg)) msg else TRUE
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - control
###

setMethod(bpschedule, "MulticoreParam",
    function(x, ...)
{
    if (.Platform$OS.type == "windows") 
        FALSE
    else
        TRUE
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - evaluation
###

setMethod(bpvec, c("ANY", "MulticoreParam"),
    function(X, FUN, ..., AGGREGATE=c, BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)

    if (!length(X))
        return(list())
    if (!bpschedule(BPPARAM))
        return(bpvec(X, FUN, ..., AGGREGATE=AGGREGATE, BPPARAM=SerialParam()))
    if (bplog(BPPARAM) || bpstopOnError(BPPARAM))
        FUN <- .composeTry(FUN, TRUE)
    else if (bpcatchErrors(BPPARAM))
        FUN <- .composeTry(FUN, FALSE)

    pvec(X, FUN, ..., AGGREGATE=AGGREGATE, mc.cores=bpworkers(BPPARAM))
})
