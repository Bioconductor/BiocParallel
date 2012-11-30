.registry <- setRefClass(".BiocParallelRegistry",
    fields = list(params = "list"),
    methods = list(
      register = function(param, default = TRUE) {
          if ((!length(param) == 1) || (!is(param, "BiocParallelParam")))
              stop("'param' must be a 'BiocParallelParam' instance")
          .self$params[[class(param)]] <- param
          if (default) {
              idx <- match(class(param), names(.self$params))
              .self$params <- c(.self$params[idx], .self$params[-idx])
          }
          invisible(registered())
      },
      registered = function(paramClass) {
          if (missing(paramClass))
              params
          else params[[paramClass]]
      }))$new()                         # Singleton

register <- .registry$register

registered <- .registry$registered
