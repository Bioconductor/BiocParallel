.MulticoreParam <- setRefClass("MulticoreParam",
    contains="BiocParallelParam",
    fields=list(
      setSeed="logical",
      recursive="logical",
      cleanup="logical",
      cleanupSignal="integer",
      verbose="logical"),
    methods=list(
     initialize = function(..., workers=detectCores(), catch.errors=TRUE,
         setSeed=TRUE, recursive=TRUE, cleanup=TRUE,
         cleanupSignal=tools::SIGTERM, verbose=FALSE)
      {
          initFields(workers=workers, catch.errors=catch.errors,
              setSeed=setSeed, recursive=recursive, cleanup=cleanup,
              cleanupSignal=cleanupSignal, verbose=verbose)
          callSuper(workers=workers, catch.errors=catch.errors, ...)
      },
      show = function() {
          callSuper()
          fields <- names(.MulticoreParam_fields())
          vals <- sapply(fields, function(fld) as.character(.self$field(fld)))
          txt <- paste(sprintf("%s: %s", fields, vals), collapse="; ")
          cat(strwrap(txt, exdent=2), sep="\n")
      }))

MulticoreParam <-
    function(workers=detectCores(), catch.errors=TRUE, setSeed=TRUE,
        recursive=TRUE, cleanup=TRUE, cleanupSignal=tools::SIGTERM,
        verbose=FALSE, ...)
{
    .MulticoreParam(workers=as.integer(workers), catch.errors=catch.errors,
        setSeed=setSeed, recursive=recursive, cleanup=cleanup,
        cleanupSignal=cleanupSignal, verbose=verbose, ...)
}

.MulticoreParam_fields <-
    function()
{
    result <- .MulticoreParam$fields()
    result[setdiff(names(result), names(.BiocParallelParam$fields()))]
}

setValidity("MulticoreParam",
    function(object)
{
    msg <- NULL
    txt <- function(fmt, flds)
        sprintf(fmt, paste(sQuote(flds), collapse=", "))

    fields <- .MulticoreParam_fields()

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
setMethod(bpmapply, c("ANY", "MulticoreParam"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
        resume=getOption("BiocParallel.resume", FALSE), BPPARAM)
{
    FUN <- match.fun(FUN)
    ## recall on subset of input data
    if (resume)
        return(.resume(FUN=FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
            USE.NAMES=USE.NAMES, BPPARAM=BPPARAM))
    ## recall in sequential
    if (!bpschedule(BPPARAM))
        return(bpmapply(FUN=FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
            USE.NAMES=USE.NAMES, resume=resume,
            BPPARAM=SerialParam(catch.errors=BPPARAM$catch.errors)))


    ## mcmapply is broken:
    ## https://bugs.r-project.org/bugzilla3/show_bug.cgi?id=15016
    ## furthermore, mcmapply is just a wrapper around mclapply
    ## -> use mclapply!
    ddd <- .getDotsForMapply(...)
    wrap <- function(.i, ...) do.call(FUN, c(lapply(ddd, "[[", .i), MoreArgs))

    ## always wrap in a try: this is the only way to throw an error for the user
    wrap <- .composeTry(wrap)

    results <- mclapply(X=seq_len(length(ddd[[1L]])), FUN=wrap,
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
    function(X, FUN, ..., AGGREGATE=c, BPPARAM)
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
