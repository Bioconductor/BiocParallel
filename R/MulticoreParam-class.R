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

setMethod(bpisup, "MulticoreParam", function(x, ...) TRUE)

setMethod(bpschedule, "MulticoreParam",
    function(x, ...)
{
    (.Platform$OS.type != "windows") && (x@recursive || !isChild())
})

## evaluation

setMethod(bplapply, c("ANY", "ANY", "MulticoreParam"),
    function(X, FUN, ..., BPPARAM)
{
    FUN <- match.fun(FUN)
    if (!bpschedule(BPPARAM))
        return(lapply(X = X, FUN = FUN, ...))

    cleanup <- if (BPPARAM@cleanup) BPPARAM@cleanupSignal else FALSE
    mclapply(X, FUN, ..., mc.set.seed=BPPARAM@setSeed,
             mc.silent=!BPPARAM@verbose, mc.cores=bpworkers(BPPARAM),
             mc.cleanup=cleanup)
})

setMethod(bpvec, c("ANY", "ANY", "MulticoreParam"),
    function(X, FUN, ..., AGGREGATE=c, BPPARAM)
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)

    if (!bpschedule(BPPARAM))
        return(FUN(X, ...))

    cleanup <- if (BPPARAM@cleanup) BPPARAM@cleanupSignal else FALSE
    pvec(X, FUN, ..., AGGREGATE=AGGREGATE,
         mc.set.seed=BPPARAM@setSeed,
         mc.silent=!BPPARAM@verbose, mc.cores=bpworkers(BPPARAM),
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
