.LastError <- setRefClass("LastError",
  fields=list(
    results="list",         # partial results
    is.error="logical",     # flags successfull/errorneous termination
    traceback="character"), # exemplary traceback
  methods=list(
    initialize = function() {
        reset()
    },
  show = function() {
      if (!length(results)) {
          cat("No partial results saved\n")
      } else {
          n.errors <- sum(is.error)
          msg <- sprintf("%i / %i partial results stored.",
                         length(is.error) - n.errors, length(is.error))
          if (n.errors > 0L) {
              n.print <- min(n.errors, 10L)
              msg <- paste(msg, sprintf("First %i error messages:\n", n.print))
              f <- function(i)
                  sprintf("[%i]: %s", i, conditionMessage(results[[i]]))
              msg <- c(msg, vapply(head(which(is.error), n.print),
                  f, character(1L)))
              cat(paste(sub("[[:space:]]+$", "", msg), collapse="\n"), "\n")
          }
      }
  },
  store = function(results, is.error, throw.error=FALSE) {
      if (any(is.error)) {
        .self$traceback <- attr(head(results[is.error], 1L)[[1L]], "traceback")
        .self$results <- replace(results, is.error,
            lapply(results[is.error], .convertToSimpleError))
        .self$is.error <- is.error
        if (throw.error) {
          msg0 <- sprintf("%d errors; first error:\n  %s", sum(is.error),
              paste(conditionMessage(.self$results[is.error][[1L]]),
                    collapse="\n  "))
          msg1 <- paste(strwrap("For more information, use bplasterror(). To
              resume calculation, re-call the function and set the argument
              'BPRESUME' to TRUE or wrap the previous call in bpresume().",
              exdent=2), collapse="\n")
          msg2 <- NULL
          if (length(.self$traceback))
              msg2 <- sprintf("First traceback:\n  %s",
                  paste(as.character(.self$traceback), collapse="\n  "))
          msg <- sub("[[:space:]]+$", "", c(msg0, msg1, msg2))
          stop(paste(msg, collapse="\n\n"), call.=FALSE)
        }
      } else {
          reset()
      }
  },
  reset = function() {
      .self$results <- list()
      .self$is.error <- NA
      .self$traceback <- character(0L)
  })
)

LastError <- .LastError()

bplasterror <-
    function()
{
    getFromNamespace("LastError", "BiocParallel")
}

.convertToSimpleError <-
    function(x)
{
    if (!is(x, "simpleError"))
        simpleError(as.character(x))
    else x
}

.try <-
    function(expr)
{
    handler_warning <-
        function(w)
    {
        cache.warnings <<- c(cache.warnings, as.character(w))
        invokeRestart("muffleWarning")
    }

    handler_error <-
        function(e)
    {
        call <- sapply(sys.calls(), deparse)
        # FIXME one might try to cleanup the traceback ...
        tb <<- capture.output(traceback(call))
        invokeRestart("abort", e)
    }

    handler_abort <- function(e) e

    tb <- NULL
    cache.warnings <- character(0L)
    x <- withRestarts(withCallingHandlers(expr, warning=handler_warning,
        error=handler_error), abort=handler_abort)

    ## we cannot use try-error or snow's internal error handling will be
    ## triggered
    if (inherits(x, "error")) {
        tmp <- x
        x <- as.character(x)
        class(x) <- "remote-error"
        attr(x, "condition") <- tmp$condition
        attr(x, "traceback") <- tb
    }
    return(x)
}

.composeTry <-
    function(FUN)
{
    FUN <- match.fun(FUN)
    function(...) .try(FUN(...))
}

.bpresume_lapply <-
    function(X, FUN, ..., BPPARAM)
{
    message("Resuming previous calculation... ")
    if (length(LastError$is.error) == 1L && is.na(LastError$is.error))
        stop("No last error caught")
    if (length(LastError$results) != length(X))
        stop("Cannot resume: length mismatch in arguments")
    results <- LastError$results
    is.error <- LastError$is.error

    next.try <- try(bplapply(X=X[is.error], FUN=FUN, ..., BPPARAM=BPPARAM,
        BPRESUME=FALSE))

    if (inherits(next.try, "try-error")) {
        LastError$store(
            results=replace(results, is.error, LastError$results),
            is.error=replace(is.error, is.error, LastError$is.error),
            throw.error=TRUE)
    } else {
        LastError$reset()
        return(replace(results, is.error, next.try))
    }
}

.bpresume_mapply <-
    function(FUN, ..., MoreArgs, SIMPLIFY, USE.NAMES, BPPARAM)
{
    message("Resuming previous calculation... ")
    if (length(LastError$is.error) == 1L && is.na(LastError$is.error))
        stop("No last error catched")
    if (length(LastError$results) != length(list(...)[[1L]]))
        stop("Cannot resume: length mismatch in arguments")
    results <- LastError$results
    is.error <- LastError$is.error
    pars <- c(list(FUN=FUN, MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
        USE.NAMES=USE.NAMES, BPRESUME=FALSE, BPPARAM=BPPARAM),
        lapply(list(...), "[", LastError$is.error))
    next.try <- try(do.call(bpmapply, pars))

    if (inherits(next.try, "try-error")) {
        LastError$store(
            results=replace(results, is.error, LastError$results),
            is.error=replace(is.error, is.error, LastError$is.error),
            throw.error=TRUE)
    } else {
        LastError$reset()
        return(replace(results, is.error, next.try))
    }
}

bpresume <-
    function(expr)
{
    prev <- getOption("BiocParallel.BPRESUME", FALSE)
    on.exit(options("BiocParallel.BPRESUME"=prev))
    options(BiocParallel.BPRESUME=TRUE)
    expr
}
