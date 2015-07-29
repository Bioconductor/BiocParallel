### =========================================================================
### MulticoreParam objects
### -------------------------------------------------------------------------

multicoreWorkers <- function() {
    cores <- 
        if (.Platform$OS.type == "windows") {
            1L
        } else {
            cores <- min(8L, parallel::detectCores() - 2L)
            if (cores <= 1L) {
                cores <- 1L
                warning(paste0("using a single core; to increase cores ",
                               "specify 'workers' e.g., ",
                               "MulticoreParam(workers = 2) "))
            }
            cores
        }
    getOption("mc.cores", cores)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor 
###

.MulticoreParam <- setRefClass("MulticoreParam",
    contains="SnowParam",
    fields=list(),
    methods=list(
        show = function() {
            callSuper()
        })
)

MulticoreParam <- function(workers=multicoreWorkers(), tasks=0L,  
        catch.errors=TRUE, stop.on.error=FALSE, 
        progressbar=FALSE, RNGseed=NULL, timeout=Inf,
        log=FALSE, threshold="INFO", logdir=NA_character_,
        resultdir=NA_character_, jobname = "BPJOB", ...)
{
    if (.Platform$OS.type == "windows")
        warning(paste0("MulticoreParam not supported on Windows. ",
                       "Use SnowParam instead."))
    if (!catch.errors)
        warning("'catch.errors' has been deprecated")

    args <- c(list(spec=workers, type="FORK"), list(...)) 
    .MulticoreParam(.clusterargs=args, cluster=.NULLcluster(),
        .controlled=TRUE, workers=as.integer(workers), 
        tasks=as.integer(tasks),
        catch.errors=catch.errors, stop.on.error=stop.on.error, 
        progressbar=progressbar, 
        RNGseed=RNGseed, timeout=timeout,
        log=log, threshold=.THRESHOLD(threshold), 
        logdir=logdir, resultdir=resultdir, jobname=jobname)
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
    function(x, ..., value)
{
    value <- as.integer(value)
    if (value > multicoreWorkers())
        stop("'value' exceeds available workers detected by multicoreWorkers()")
 
    x$workers <- value 
    x$.clusterargs$spec <- value 
    x 
})

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
    function(X, FUN, ..., AGGREGATE=c, BPREDO=list(), BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)

    if (!length(X))
        return(list())
    if (!bpschedule(BPPARAM))
        return(bpvec(X, FUN, ..., AGGREGATE=AGGREGATE, BPREDO=BPREDO,
               BPPARAM=SerialParam()))
    if (bplog(BPPARAM) || bpstopOnError(BPPARAM))
        FUN <- .composeTry(FUN, TRUE)
    else
        FUN <- .composeTry(FUN, FALSE)

    if (length(BPREDO)) {
        if (all(idx <- !bpok(BPREDO)))
            stop("no error detected in 'BPREDO'")
        if (length(BPREDO) != length(X))
            stop("length(BPREDO) must equal length(X)")
        message("Resuming previous calculation ... ")
        res <- pvec(X[idx], FUN, ..., AGGREGATE=AGGREGATE, 
                   mc.cores=bpworkers(BPPARAM))
        BPREDO[id] <- res
        BPREDO
    }
    pvec(X, FUN, ..., AGGREGATE=AGGREGATE, mc.cores=bpworkers(BPPARAM))
})
