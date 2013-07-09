.LastError = setRefClass("LastError",
  fields = list(
    obj = "ANY",           # object function applied to
    results = "list",      # partial results
    is.error = "logical",  # flags successfull/errorneous termination
    extra = "list"),       # slot for extra stuff the user wants to retrieve, i.e. a BatchJobs registry
  methods = list(
    initialize = function() {
      reset()
    },
    show = function() {
      if (is.null(obj)) {
        message("No partial results saved")
      } else {
        n = sum(is.error)
        msg = sprintf("%i/%i partial results stored.", n, length(obj))
        if (n) {
          n.print = min(n, 10L)
          msg = paste(msg, sprintf("First %i error messages:", n.print))
          msg = c(msg, vapply(head(which(is.error), n.print),
                              function(i) sprintf("[%i]: %s", i, as.character(results[i])),
                              character(1L)))
          message(paste(msg, collapse = "\n"))
        }
      }
    },
    store = function(obj, results, is.error, extra = list(), throw.error = FALSE) {
      .self$obj = obj
      .self$results = results
      .self$is.error = is.error
      .self$extra = extra
      if (throw.error) {
        msg = c("Errors occurred during execution. First error message:",
                as.character(results[is.error][[1L]]),
                "You can resume calculation by re-calling 'bplapply' with 'LastError' as first argument.")
        stop(paste(msg, collapse = "\n"))
      }
    },
    reset = function() {
      .self$obj = NULL
      .self$results = list()
      .self$is.error = NA
      .self$extra = list()
      invisible(TRUE)
    }
  )
)
LastError = .LastError()
