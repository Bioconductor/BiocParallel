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
    store = function(obj, results, is.error, ...) {
      .self$obj = obj
      .self$results = results
      .self$is.error = is.error
      .self$extra = list(...)
      invisible(TRUE)
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
