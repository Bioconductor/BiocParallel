.LastError = setRefClass("LastError",
  fields = list(
    results = "list",      # partial results
    is.error = "logical"), # flags successfull/errorneous termination
  methods = list(
    initialize = function() {
      reset()
    },
    show = function() {
      if (!length(results)) {
        message("No partial results saved")
      } else {
        n.errors = sum(is.error)
        msg = sprintf("%i/%i partial results stored.", length(is.error) - n.errors, length(is.error))
        if (n.errors > 0L) {
          n.print = min(n.errors, 10L)
          msg = paste(msg, sprintf("First %i error messages:", n.print))
          msg = c(msg, vapply(head(which(is.error), n.print),
                              function(i) sprintf("[%i]: %s", i, as.character(results[[i]])),
                              character(1L)))
          message(paste(msg, collapse = "\n"))
        }
      }
    },
    store = function(results, is.error, throw.error = FALSE) {
      if (any(is.error)) {
        .self$results = replace(results, is.error, lapply(results[is.error], .convertToSimpleError))
        .self$is.error = is.error
        if (throw.error) {
          msg = c("Errors occurred during execution. First error message:",
                  as.character(results[is.error][[1L]]),
                  "For more information, use getLastError().",
                  "To resume calculation, re-call the function and set the argument 'resume' to TRUE.")
          stop(paste(msg, collapse = "\n"))
        }
      } else {
        reset()
      }
    },
    reset = function() {
      .self$results = list()
      .self$is.error = NA
    }
  )
)
LastError = .LastError()

getLastError = function() {
  getFromNamespace("LastError", "BiocParallel")
}

.convertToSimpleError = function(x) {
  simpleError(as.character(x))
}

.throwError = function(x) {
  x = as.character(x)
  stop(simpleError(x))
}

.try = function(expr) {
  handler_warning = function(w) {
    cache.warnings <<- c(cache.warnings, as.character(w))
    invokeRestart("muffleWarning")
  }

  handler_error = function(e) {
    call = sapply(sys.calls(), deparse)
    # FIXME one might try to cleanup the traceback ...
    tb <<- capture.output(print(traceback(call)))
    invokeRestart("abort", e)
  }

  handler_abort = function(e) e

  tb = NULL
  cache.warnings = character(0L)
  x = withRestarts(withCallingHandlers(expr, warning = handler_warning, error = handler_error), abort = handler_abort)

  # try to mimic the try return type
  # this may be incomplete...
  if (inherits(x, "error")) {
    tmp = x
    x = as.character(x)
    class(x) = "remote-error"
    attr(x, "condition") = tmp$condition
    attr(x, "traceback") = tb
  }
  if (length(cache.warnings)) {
    class(x) = c(class(x), "try-warning")
    attr(x, "warnings") = cache.warnings
  }
  return(x)
}

.composeTry = function(FUN) {
  FUN = match.fun(FUN)
  function(...) .try(FUN(...))
}

.resume = function(FUN, ..., MoreArgs, SIMPLIFY, USE.NAMES, BPPARAM) {
  message("Resuming previous calculation... ")
  if (length(LastError$results) != length(list(...)[[1L]]))
    stop("Cannot resume: Length mismatch in arguments")
  is.error = LastError$is.error
  results = LastError$results
  # we have a possibly unnecessary copy here
  # change the signature to support subsets of args?
  pars = c(list(FUN=FUN, MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
                USE.NAMES=USE.NAMES, resume=FALSE, BPPARAM=BPPARAM),
           lapply(list(...), "[", LastError$is.error))
  next.try = do.call(bpmapply, pars)

  if (inherits(next.try, "try-error")) {
    LastError$results = replace(results, is.error, LastError$results)
    LastError$is.error = replace(is.error, is.error, LastError$is.error)
    stop(as.character(next.try))
  } else {
    LastError$reset()
    return(replace(results, is.error, next.try))
  }
}
