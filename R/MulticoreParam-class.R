### =========================================================================
### MulticoreParam objects
### -------------------------------------------------------------------------

multicoreWorkers <- function() {
    cores <- 
        if (.Platform$OS.type == "windows") {
            1L
        } else {
            max(1L, parallel::detectCores() - 2L)
        }
    getOption("mc.cores", cores)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor 
###

.MulticoreParam <- setRefClass("MulticoreParam",
    contains="SnowParam",
    fields=list(),
    methods=list()
)

MulticoreParam <- function(workers=multicoreWorkers(), tasks=0L,  
        catch.errors=TRUE, stop.on.error=TRUE, 
        progressbar=FALSE, RNGseed=NULL, timeout=Inf,
        log=FALSE, threshold="INFO", logdir=NA_character_,
        resultdir=NA_character_, jobname = "BPJOB", ...)
{
    if (.Platform$OS.type == "windows")
        warning("MulticoreParam() not supported on Windows, use SnowParam()")

    if (!missing(catch.errors))
        warning("'catch.errors' is deprecated, use 'stop.on.error'")

    args <- c(list(spec=workers, type="FORK"), list(...)) 
    .MulticoreParam(.clusterargs=args, cluster=.NULLcluster(),
        .controlled=TRUE, workers=as.integer(workers), 
        tasks=as.integer(tasks),
        catch.errors=catch.errors, stop.on.error=stop.on.error, 
        progressbar=progressbar, 
        RNGseed=RNGseed, timeout=timeout,
        log=log, threshold=threshold, logdir=logdir,
        resultdir=resultdir, jobname=jobname)
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

setReplaceMethod("bpworkers", c("MulticoreParam", "numeric"),
    function(x, value)
{
    value <- as.integer(value)
    if (value > multicoreWorkers())
        stop("'value' exceeds available workers detected by multicoreWorkers()")
 
    x$workers <- value 
    x$.clusterargs$spec <- value 
    x 
})

setMethod("bpschedule", "MulticoreParam",
    function(x)
{
    if (.Platform$OS.type == "windows") 
        FALSE
    else
        TRUE
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - evaluation
###

setMethod("bpvec", c("ANY", "MulticoreParam"),
    function(X, FUN, ..., AGGREGATE=c, BPREDO=list(), BPPARAM=bpparam())
{
    if (!length(X))
        return(list())

    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)

    if (!bpschedule(BPPARAM))
        return(bpvec(X, FUN, ..., AGGREGATE=AGGREGATE, BPREDO=BPREDO,
               BPPARAM=SerialParam()))

    idx <- .redo_index(X, BPREDO)
    if (any(idx))
        X <- X[idx]

    FUN <- .composeTry(FUN, bplog(BPPARAM), bpstopOnError(BPPARAM),
                       timeout=bptimeout(BPPARAM))

    res <- pvec(X, FUN, ..., AGGREGATE=AGGREGATE, mc.cores=bpworkers(BPPARAM))

    names(res) <- names(X)

    if (any(idx)) {
        BPREDO[idx] <- res
        res <- BPREDO 
    }

    if (!all(bpok(res)))
        stop(.error_bplist(res))

    res
})
