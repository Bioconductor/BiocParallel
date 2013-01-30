.MulticoreParam <- setClass("MulticoreParam",
    representation(
        setSeed = "logical",
        recursive = "logical",
        cleanup = "logical",
        cleanupSignal = "integer",
        verbose = "logical"),
    prototype(
        workers = getOption("mc.cores", 2L),
        setSeed = TRUE,
        recursive = TRUE,
        cleanup=TRUE,
        cleanupSignal = tools::SIGTERM,
        verbose = FALSE),
    "BiocParallelParam")

MulticoreParam <-
    function(workers=detectCores(), setSeed=TRUE, recursive=TRUE,
             cleanup=TRUE, cleanupSignal=tools::SIGTERM,
             verbose=FALSE, ...)
{
    workers <- as.integer(workers)
    .MulticoreParam(workers=workers, setSeed=setSeed, recursive=recursive,
                    cleanup=cleanup, cleanupSignal=cleanupSignal,
                    verbose=verbose, ...)
}

setValidity("MulticoreParam",
    function(object)
{
    msg <- NULL

    slts <- sapply(slotNames(object), slot, object=object)
    isScalar <- sapply(slts, length) == 1L
    if (!all(isScalar)) {
        txt <- sprintf("%s must be length 1",
                       paste(sQuote(names(slts)[!isScalar]), collapse=", "))
        msg <- c(msg, txt)
    }

    if (!is.null(msg)) msg else TRUE
})

## control

setMethod(bpisup, "MulticoreParam", function(param, ...) TRUE)

setMethod(bpschedule, "MulticoreParam",
    function(param, ...)
{
    (.Platform$OS.type != "windows") && (param@recursive || !isChild())
})

## evaluation

setMethod(bplapply, c("ANY", "ANY", "MulticoreParam"),
    function(X, FUN, ..., param)
{
    FUN <- match.fun(FUN)
    if (!bpschedule(param))
        return(lapply(X = X, FUN = FUN, ...))

    cleanup <- if (param@cleanup) param@cleanupSignal else FALSE
    mclapply(X, FUN, ..., mc.set.seed=param@setSeed,
             mc.silent=!param@verbose, mc.cores=bpworkers(param),
             mc.cleanup=cleanup)
})

setMethod(bpvec, c("ANY", "ANY", "MulticoreParam"),
    function(X, FUN, ..., AGGREGATE=c, param)
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)

    if (!bpschedule(param))
        return(FUN(X, ...))

    cleanup <- if (param@cleanup) param@cleanupSignal else FALSE
    pvec(X, FUN, ..., AGGREGATE=AGGREGATE,
         mc.set.seed=param@setSeed,
         mc.silent=!param@verbose, mc.cores=bpworkers(param),
         mc.cleanup=cleanup)
})

setMethod(show, "MulticoreParam",
    function(object)
{
    callNextMethod()
    txt <- sapply(slotNames(object), function(nm) {
        paste(nm, slot(object, nm), sep=": ")
    })
    cat(strwrap(paste(txt, collapse="; "), exdent=2), sep="\n")
})
