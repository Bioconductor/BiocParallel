.MulticoreParam <-
    setRefClass("MulticoreParam",
                contains="BiocParallelParam",
                fields=list(
                setSeed = "logical",
                recursive = "logical",
                cleanup = "logical",
                cleanupSignal = "integer",
                verbose = "logical"))

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

    fields <- names(x$.refClassDef@fieldClasses)
    for (f in fields) {
        if (length(object$field(f)) != 1) {
            msg <- c(msg, sprintf("%s must be length 1", f))
        } else if (is.na(object$field(f))) {
            msg <- c(msg, sprintf("%s must not be NA", f))
        }
    }
    if (!is.null(msg)) msg else TRUE
})

## control

setMethod(bpisup, "MulticoreParam", function(x, ...) TRUE)

setMethod(bpschedule, "MulticoreParam",
    function(x, ...)
{
    (.Platform$OS.type != "windows") && (x$recursive || !isChild())
})

## evaluation

setMethod(bplapply, c("ANY", "MulticoreParam"),
    function(X, FUN, ..., BPPARAM)
{
    FUN <- match.fun(FUN)
    if (!bpschedule(BPPARAM))
        return(lapply(X = X, FUN = FUN, ...))

    cleanup <- if (BPPARAM$cleanup) BPPARAM$cleanupSignal else FALSE
    mclapply(X, FUN, ..., mc.set.seed=BPPARAM$setSeed,
             mc.silent=!BPPARAM$verbose, mc.cores=bpworkers(BPPARAM),
             mc.cleanup=cleanup)
})

setMethod(bpvec, c("ANY", "MulticoreParam"),
    function(X, FUN, ..., AGGREGATE=c, BPPARAM)
{
    FUN <- match.fun(FUN)
    AGGREGATE <- match.fun(AGGREGATE)

    if (!bpschedule(BPPARAM))
        return(FUN(X, ...))

    cleanup <- if (BPPARAM$cleanup) BPPARAM$cleanupSignal else FALSE
    pvec(X, FUN, ..., AGGREGATE=AGGREGATE,
         mc.set.seed=BPPARAM$setSeed,
         mc.silent=!BPPARAM$verbose, mc.cores=bpworkers(BPPARAM),
         mc.cleanup=cleanup)
})

setMethod(show, "MulticoreParam",
    function(object)
{
    callNextMethod()
    txt <- sapply(names(object$.refClassDef@fieldClasses), function(nm) {
        sprintf("%s: %s", nm, object$field(nm))
    })
    cat(strwrap(paste(txt, collapse="; "), exdent=2), sep="\n")
})
