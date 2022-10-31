### =========================================================================
### .registry object
### -------------------------------------------------------------------------

.registry <- setRefClass(".BiocParallelRegistry",
    fields=list(
        bpparams = "list"),
    methods=list(
        register = function(BPPARAM, default = TRUE) {
            BPPARAM <- eval(BPPARAM)
            if ((!length(BPPARAM) == 1) || !is(BPPARAM, "BiocParallelParam"))
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
                .self$bpparams
            else .self$bpparams[[bpparamClass]]
        })
)$new()  # Singleton

.register <- .registry$register

.registered <- .registry$registered

.registry_init <- function() {
    multicore <- .defaultWorkers() > 1L
    tryCatch({
        if ((.Platform$OS.type == "windows") && multicore) {
            .register(getOption(
                "SnowParam",
                SnowParam()
            ), TRUE)
            .register(getOption("SerialParam", SerialParam()), FALSE)
        } else if (multicore) {
            ## linux / mac
            .register(getOption(
                "MulticoreParam",
                MulticoreParam()
            ), TRUE)
            .register(getOption(
                "SnowParam",
                SnowParam()
            ), FALSE)
            .register(getOption("SerialParam", SerialParam()), FALSE)
        } else {
            .register(getOption("SerialParam", SerialParam()), TRUE)
        }
    }, error=function(err) {
        message(
            "'BiocParallel' did not register default BiocParallelParam:\n",
            "  ", conditionMessage(err)
        )
        NULL
    })
}

register <- function(BPPARAM, default = TRUE) {
    if (length(.registry$bpparams) == 0L)
        .registry_init()
    .register(BPPARAM, default = default)
}

registered <- function(bpparamClass) {
    if (length(.registry$bpparams) == 0L)
        .registry_init()
    .registered(bpparamClass)
}

bpparam <- function(bpparamClass) {
    if (missing(bpparamClass))
        bpparamClass <- names(registered())[1]
    default <- registered()[[bpparamClass]]
    result <- getOption(bpparamClass, default)
    if (is.null(result))
        stop("BPPARAM '", bpparamClass,
             "' not registered() or in names(options())")
    result
}
