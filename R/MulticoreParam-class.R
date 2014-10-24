multicoreWorkers <- function() {
    cores <- if (.Platform$OS.type == "windows")
        1
    else
        min(8L, detectCores())
    getOption("mc.cores", cores)
}


.MulticoreParam <- setRefClass("MulticoreParam",
    contains="BiocParallelParam",
    fields=list(
      setSeed="logical",
      recursive="logical",
      cleanup="logical",
      cleanupSignal="integer",
      verbose="logical"),
    methods=list(
     initialize = function(..., workers=multicoreWorkers(),
         catch.errors=TRUE, setSeed=TRUE, recursive=TRUE, cleanup=TRUE,
         cleanupSignal=tools::SIGTERM, verbose=FALSE)
     {
          initFields(workers=workers, catch.errors=catch.errors,
              setSeed=setSeed, recursive=recursive, cleanup=cleanup,
              cleanupSignal=cleanupSignal, verbose=verbose)
          callSuper(workers=workers, catch.errors=catch.errors, ...)
      },
      show = function() {
          callSuper()
          fields <- names(.paramFields(.MulticoreParam))
          vals <- sapply(fields, function(fld) as.character(.self$field(fld)))
          txt <- paste(sprintf("%s: %s", fields, vals), collapse="; ")
          cat(strwrap(txt, exdent=2), sep="\n")
      }))

MulticoreParam <-
    function(workers=multicoreWorkers(), catch.errors=TRUE, setSeed=TRUE,
        recursive=TRUE, cleanup=TRUE, cleanupSignal=tools::SIGTERM,
        verbose=FALSE, ...)
{
    .MulticoreParam(workers=as.integer(workers), catch.errors=catch.errors,
        setSeed=setSeed, recursive=recursive, cleanup=cleanup,
        cleanupSignal=cleanupSignal, verbose=verbose, ...)
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

## control

setMethod(bpisup, "MulticoreParam", function(x, ...) TRUE)

setMethod(bpschedule, "MulticoreParam",
    function(x, ...)
{
    (.Platform$OS.type != "windows") && (x$recursive || !isChild())
})

## evaluation
setMethod(bplapply, c("ANY", "MulticoreParam"),
    function(X, FUN, ..., BPRESUME=getOption("BiocParallel.BPRESUME", FALSE),
        BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    ## recall on subset of input data
    if (BPRESUME)
        return(.bpresume_lapply(X=X, FUN=FUN, ..., BPPARAM=BPPARAM))
    ## recall in sequential
    if (!bpschedule(BPPARAM))
        return(bplapply(X=X, FUN=FUN, ..., BPRESUME=BPRESUME,
            BPPARAM=SerialParam(catch.errors=BPPARAM$catch.errors)))


    ## always wrap in a try: this is the only way to throw an error for the user
    FUN <- .composeTry(FUN)

    results <- mclapply(X=X, FUN=FUN, ...,
        mc.set.seed=BPPARAM$setSeed, mc.silent=!BPPARAM$verbose,
        mc.cores=bpworkers(BPPARAM),
        mc.cleanup=if (BPPARAM$cleanup) BPPARAM$cleanupSignal else FALSE)

    is.error <- vapply(results, inherits, logical(1L), what="remote-error")
    if (any(is.error)) {
        if (BPPARAM$catch.errors)
            LastError$store(results=results, is.error=is.error,
                throw.error=TRUE)
        stop(as.character(results[[head(which(is.error), 1L)]]))
    }

    results
})

setMethod(bpmapply, c("ANY", "MulticoreParam"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
        BPRESUME=getOption("BiocParallel.BPRESUME", FALSE), BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    ## recall on subset of input data
    if (BPRESUME)
        return(.bpresume_mapply(FUN=FUN, ..., MoreArgs=MoreArgs,
            SIMPLIFY=SIMPLIFY, USE.NAMES=USE.NAMES, BPPARAM=BPPARAM))
    ## recall in sequential
    if (!bpschedule(BPPARAM))
        return(bpmapply(FUN=FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
            USE.NAMES=USE.NAMES, BPRESUME=BPRESUME,
            BPPARAM=SerialParam(catch.errors=BPPARAM$catch.errors)))


    ## mcmapply is broken:
    ## https://bugs.r-project.org/bugzilla3/show_bug.cgi?id=15016
    ## furthermore, mcmapply is just a wrapper around mclapply
    ## -> use mclapply!

    ddd <- .getDotsForMapply(...)
    if (!length(ddd) || !length(ddd[[1L]]))
      return(list())

    ## always wrap in a try: this is the only way to throw an error for the user
    wrap <- .composeTry(function(.i, .FUN, .ddd, .MoreArgs) {
      dots <- lapply(.ddd, `[`, .i)
      .mapply(.FUN, dots, .MoreArgs)[[1L]]
    })

    results <- mclapply(X=seq_along(ddd[[1L]]), FUN=wrap,
        .FUN=FUN, .ddd=ddd, .MoreArgs=MoreArgs,
        mc.set.seed=BPPARAM$setSeed, mc.silent=!BPPARAM$verbose,
        mc.cores=bpworkers(BPPARAM),
        mc.cleanup=if (BPPARAM$cleanup) BPPARAM$cleanupSignal else FALSE)
    results <- .rename(results, ddd, USE.NAMES=USE.NAMES)

    is.error <- vapply(results, inherits, logical(1L), what="remote-error")
    if (any(is.error)) {
        if (BPPARAM$catch.errors)
            LastError$store(results=results, is.error=is.error,
                throw.error=TRUE)
        stop(as.character(results[[head(which(is.error), 1L)]]))
    }

    .simplify(results, SIMPLIFY=SIMPLIFY)
})

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
    ## BPRESUME not used

    ## always wrap in a try: only way to throw an error for the user
    ITER <- .composeTry(ITER)  ## necessary on master?
    FUN <- .composeTry(FUN)

    if (.Platform$OS.type == "windows")
        results <- .bpiterate_serial(ITER, FUN, ...)
    else
        results <- .bpiterate_multicore(ITER, FUN, ...,
            mc.set.seed=BPPARAM$setSeed, mc.silent=!BPPARAM$verbose,
            mc.cores=bpworkers(BPPARAM), 
            mc.cleanup=if (BPPARAM$cleanup) BPPARAM$cleanupSignal else FALSE)

    is.error <- vapply(results, inherits, logical(1L), what="remote-error")
    if (any(is.error)) {
        if (BPPARAM$catch.errors)
            LastError$store(results=results, is.error=is.error,
                throw.error=TRUE)
        stop(as.character(results[[head(which(is.error), 1L)]]))
    }

    results

})

