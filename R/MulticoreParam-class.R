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
        setSeed="logical", ## FIXME: remove when mclapply goes away
        recursive="logical",
        cleanup="logical",
        cleanupSignal="integer",
        verbose="logical")
)

MulticoreParam <- function(workers=multicoreWorkers(), 
        stopOnError=FALSE, log=FALSE, threshold="INFO", logdir=character(),
        resultdir=character(), setSeed=TRUE,
        recursive=TRUE, cleanup=TRUE, cleanupSignal=tools::SIGTERM,
        verbose=FALSE, ...)
{
    .MulticoreParam(setSeed=setSeed, recursive=recursive, cleanup=cleanup,
        cleanupSignal=cleanupSignal, verbose=verbose,
        workers=as.integer(workers), stopOnError=stopOnError, log=log,
        threshold=threshold, logdir=logdir, resultdir=resultdir,
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
    (.Platform$OS.type != "windows") && (x$recursive || !isChild())
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - evaluation
###

setMethod(bplapply, c("ANY", "MulticoreParam"),
    function(X, FUN, ..., BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    ## no scheduling -> serial evaluation 
    if (!bpschedule(BPPARAM))
        return(bplapply(X=X, FUN=FUN, ..., BPPARAM=SerialParam()))
    if (!bpisup(BPPARAM)) {
        BPPARAM <- bpstart(BPPARAM)
        on.exit(bpstop(BPPARAM))
    }
    cl <- bpbackend(BPPARAM)
    argfun <- function(i) c(list(X[[i]]), list(...))
    bpdynamicClusterApply(cl, FUN, length(X), names(X), argfun, BPPARAM) 
})

setMethod(bpiterate, c("ANY", "ANY", "MulticoreParam"),
    function(ITER, FUN, ..., BPPARAM=bpparam())
{
    ITER <- match.fun(ITER)
    FUN <- match.fun(FUN)

    if (.Platform$OS.type == "windows")
        results <- .bpiterate_serial(ITER, FUN, ...)
    else
        results <- .bpiterate_multicore(ITER, FUN, ...,
            mc.set.seed=BPPARAM$setSeed, mc.silent=!BPPARAM$verbose,
            mc.cores=bpworkers(BPPARAM), 
            mc.cleanup=if (BPPARAM$cleanup) BPPARAM$cleanupSignal else FALSE)

    results

})

