.MulticoreParam <- setClass("MulticoreParam",
    representation(
        cores = "integer",
        setSeed = "logical",
        recursive = "logical",
        cleanup = "logical",
        cleanupSignal = "integer",
        verbose = "logical"),
    prototype(
        cores = getOption("mc.cores", 2L),
        setSeed = TRUE,
        recursive = TRUE,
        cleanup=TRUE,
        cleanupSignal = tools::SIGTERM,
        verbose = FALSE),
    "BiocParallelParam")

MulticoreParam <-
    function(cores=detectCores(), setSeed=TRUE, recursive=TRUE,
             cleanup=TRUE, cleanupSignal=tools::SIGTERM,
             verbose=FALSE, ...)
{
    cores <- as.integer(cores)
    .MulticoreParam(cores=cores, setSeed=setSeed, recursive=recursive,
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
    
    if (isScalar[["cores"]] && (object@cores < 1L))
        msg <- c(msg, "'cores' must be integer(1), > 0")

    if (!is.null(msg)) msg else TRUE
})

setMethod(bplapply, c("ANY", "ANY", "MulticoreParam"),
    function(X, FUN, ..., param)
{
    FUN <- match.fun(FUN)
    if (isChild() && !isTRUE(param@recursive))
        return(lapply(X = X, FUN = FUN, ...))

    cleanup <- if (param@cleanup) param@cleanupSignal else FALSE
    mclapply(X, FUN, ..., mc.set.seed=param@setSeed,
             mc.silent=!param@verbose, mc.cores=param@cores,
             mc.cleanup=cleanup)
})

setMethod(bpvec, c("ANY", "ANY", "MulticoreParam"),
    function(X, FUN, ..., param)
{
    FUN <- match.fun(FUN)
    if (isChild() && !isTRUE(param@recursive))
        return(FUN(X, ...))

    cleanup <- if (param@cleanup) param@cleanupSignal else FALSE
    pvec(X, FUN, ..., mc.set.seed=param@setSeed,
         mc.silent=!param@verbose, mc.cores=param@cores,
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
