.registry <- setRefClass(".BiocParallelRegistry",
    fields=list(bpparams = "list"),
    methods=list(
      register = function(BPPARAM, default = TRUE) {
          if ((!length(BPPARAM) == 1) || (!is(BPPARAM, "BiocParallelParam")))
              stop("'BPPARAM' must be a 'BiocParallelParam' instance")
          .self$bpparams[[class(BPPARAM)]] <- BPPARAM
          if (default) {
              idx <- match(class(BPPARAM), names(.self$bpparams))
              .self$bpparams <- c(.self$bpparams[idx], .self$bpparams[-idx])
          }
          invisible(registered())
      },
      registered = function(bpparamClass) {
          if (missing(bpparamClass))
              bpparams
          else bpparams[[bpparamClass]]
      }))$new()                         # Singleton

register <- .registry$register

registered <- .registry$registered
