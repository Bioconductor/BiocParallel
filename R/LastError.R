.LastError = setRefClass("LastError",
  fields = list(
    args = "list",         # list of arguments to iterate over. all must have same length!
    results = "list",      # partial results
    is.error = "logical",  # flags successfull/errorneous termination
    MoreArgs = "list"),    # more arguments
  methods = list(
    initialize = function() {
      reset()
    },
    show = function() {
      if (is.null(args)) {
        message("No partial results saved")
      } else {
        n.errors = sum(is.error)
        msg = sprintf("%i/%i partial results stored.", n.errors, length(is.error))
        if (n.errors) {
          n.print = min(n.errors, 10L)
          msg = paste(msg, sprintf("First %i error messages:", n.print))
          msg = c(msg, vapply(head(which(is.error), n.print),
                              function(i) sprintf("[%i]: %s", i, as.character(results[[i]])),
                              character(1L)))
          message(paste(msg, collapse = "\n"))
        }
      }
    },
    store = function(args, results, is.error, MoreArgs = list(), throw.error = FALSE) {
      if (any(is.error)) {
        .self$args = args
        .self$results = replace(results, is.error, lapply(results[is.error], .convertToSimpleError))
        .self$is.error = is.error
        .self$MoreArgs = MoreArgs
        if (throw.error) {
          msg = c("Errors occurred during execution. First error message:",
                  as.character(results[is.error][[1L]]),
                  "You can resume calculation by re-calling 'bplapply' with 'LastError' as first argument.")
          stop(paste(msg, collapse = "\n"))
        }
      } else {
        reset()
      }
    },
    reset = function() {
      .self$args = list()
      .self$results = list()
      .self$is.error = NA
      .self$MoreArgs = list()
      invisible(TRUE)
    }
  )
)
LastError = .LastError()

getLastError = function() {
  getFromNamespace("LastError", "BiocParallel")
}

.convertToSimpleError = function(x) {
  x = as.character(x)
  simpleError(x)
}

.try = function(expr, debug=FALSE) {
  if (!debug)
    return(try(expr))
  handler_warning = function(w) {
    cache.warnings <<- c(cache.warnings, w)
    invokeRestart("muffleWarning")
  }

  handler_error = function(e) {
    catched.error <<- TRUE
    dump.frames(".lastDump")
    dumped = get(".lastDump", envir = .GlobalEnv)
    rm(".lastDump", envir = .GlobalEnv)
    call = sapply(sys.calls(), deparse)
    invokeRestart("abort", e, call, dumped)
  }

  handler_abort = function(e, call, dumped) {
    list(value = e, call = call, dump = dumped)
  }

  catched.error = FALSE
  cache.warnings = list()
  ret = setNames(vector("list", 5L), c("value", "warnings", "error", "call", "dump"))

  x = withRestarts(withCallingHandlers(expr, warning = handler_warning, error = handler_error), abort = handler_abort)
  if (catched.error) {
    ret[names(x)] = x
    class(ret) = "try-error"
  } else {
    ret[["value"]] = x
  }
  ret[["warnings"]] = cache.warnings
  ret
}
