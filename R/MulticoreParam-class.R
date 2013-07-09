.MulticoreParam <- setRefClass("MulticoreParam",
    contains="BiocParallelParam",
    fields=list(
      setSeed = "logical",
      recursive = "logical",
      cleanup = "logical",
      cleanupSignal = "integer",
      verbose = "logical"),
    methods=list(
      initialize = function(workers=detectCores(), setSeed=TRUE,
          recursive=TRUE, cleanup=TRUE, cleanupSignal=tools::SIGTERM,
          verbose=FALSE, ...)
      {
          initFields(workers=workers, setSeed=setSeed,
                     recursive=recursive, cleanup=cleanup,
                     cleanupSignal=cleanupSignal, verbose=verbose)
          callSuper(workers=workers, ...)
      },
      show = function() {
          callSuper()
          fields <- names(.MulticoreParam_fields())
          vals <- sapply(fields, function(fld) as.character(.self$field(fld)))
          txt <- paste(sprintf("%s: %s", fields, vals), collapse="; ")
          cat(strwrap(txt, exdent=2), sep="\n")
      }))

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

setMethod(bplapply, c("ANY", "MulticoreParam"),
    function(X, FUN, ..., BPPARAM)
{
    FUN <- match.fun(FUN)
    if (!bpschedule(BPPARAM))
        return(lapply(X = X, FUN = FUN, ...))

    cleanup <- if (BPPARAM$cleanup) BPPARAM$cleanupSignal else FALSE
    results <- mclapply(X, FUN, ..., mc.set.seed=BPPARAM$setSeed,
                        mc.silent=!BPPARAM$verbose, mc.cores=bpworkers(BPPARAM),
                        mc.cleanup=cleanup)
    is.error <- vapply(results, inherits, TRUE, what = "try-error")
    if (any(is.error)) {
      if (all(is.error))
        stop("All cores produced errors. First error message:\n",
             as.character(results[[1L]]),
             "You can retrieve other error messages from 'LastError'")

      # -> save partial results in LastError
      LastError$store(obj = X, results = results, is.error = is.error)
      msg = strwrap(c("Errors occurred during execution. First error message:",
                      as.character(results[is.error][[1L]]),
                      "You can resume calculation by re-calling 'bplapply' with 'LastError' as first argument."),
                    indent = 2L)
      stop(paste(msg, collapse = "\n"))
    }
    return(results)
})

# re-implementation necessary to include LastError
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
