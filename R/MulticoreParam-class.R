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
        setSeed="logical",
        recursive="logical",
        cleanup="logical",
        cleanupSignal="integer",
        verbose="logical"),
    methods=list(
      initialize = function(..., 
          setSeed=TRUE,
          recursive=TRUE,
          cleanup=TRUE,
          cleanupSignal=tools::SIGTERM,
          verbose=FALSE)
      { 
          initFields(setSeed=setSeed, recursive=recursive, cleanup=cleanup, 
                     cleanupSignal=cleanupSignal, verbose=verbose)
          callSuper(...)
      })
)

MulticoreParam <- function(workers=multicoreWorkers(), 
        catch.errors=TRUE, stop.on.error=FALSE, 
        log=FALSE, threshold="INFO", logdir=character(),
        resultdir=character(), setSeed=TRUE,
        recursive=TRUE, cleanup=TRUE, cleanupSignal=tools::SIGTERM,
        verbose=FALSE, ...)
{
    .MulticoreParam(setSeed=setSeed, recursive=recursive, cleanup=cleanup,
        cleanupSignal=cleanupSignal, verbose=verbose,
        workers=as.integer(workers), 
        catch.errors=catch.errors, stop.on.error=stop.on.error, 
        log=log, threshold=threshold, logdir=logdir, resultdir=resultdir,
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

setMethod(bpvec, c("ANY", "MulticoreParam"),
    function(X, FUN, ..., AGGREGATE=c, BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)

    if (!bpschedule(BPPARAM))
        return(FUN(X, ...))

    pvec(X, FUN, ..., AGGREGATE=AGGREGATE,
         mc.set.seed=BPPARAM$setSeed,
         mc.silent=!BPPARAM$verbose, mc.cores=bpworkers(BPPARAM),
         mc.cleanup=if (BPPARAM$cleanup) BPPARAM$cleanupSignal else FALSE)
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

