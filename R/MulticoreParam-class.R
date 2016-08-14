### =========================================================================
### MulticoreParam objects
### -------------------------------------------------------------------------

multicoreWorkers <- function() {
    if (.Platform$OS.type == "windows") {
        cores <- 1L
    } else {
        cores <- max(1L, parallel::detectCores() - 2L)
        if (nzchar("BBS_HOME"))
            cores <- min(4L, cores)
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
        progressbar=FALSE, RNGseed=NULL, timeout= 30L * 24L * 60L * 60L,
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
