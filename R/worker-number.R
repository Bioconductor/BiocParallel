.workerEnvironmentVariable <-
    function(variable, default = NA_integer_)
{
    result <- withCallingHandlers({
        value <- Sys.getenv(variable, default)
        as.integer(value)
    }, warning = function(w) {
        txt <- sprintf(
            paste0(
                "Trying to coercing the environment variable '%s' to an ",
                "integer caused a warning. The value of the environment ",
                "variable was '%s'. The warning was: %s"
            ),
            variable, value, conditionMessage(w)
        )
        .warning(txt)
        invokeRestart("muffleWarning")
    })

    if (!is.na(result) && (result <= 0L)) {
        txt <- sprintf(
            "The environment variable '%s' must be > 0. The value was '%d'.",
            variable, result
        )
        .stop(txt)
    }

    result
}

.defaultWorkers <-
    function()
{
    ## assign default cores
    ## environment variables; least to most compelling
    result <- .workerEnvironmentVariable("R_PARALLELLY_AVAILABLECORES_FALLBACK")
    max_number <- .workerEnvironmentVariable("BIOCPARALLEL_WORKER_MAX", result)
    default_number <-
        .workerEnvironmentVariable("BIOCPARALLEL_WORKER_NUMBER", result)
    if (is.na(max_number)) {
        result <- default_number
    } else {
        result <- min(max_number, default_number, na.rm = TRUE)
    }

    ## fall back to detectCores() if necessary
    if (is.na(result)) {
        result <- parallel::detectCores()
        if (is.na(result))
            result <- 1L
        result <- max(1L, result - 2L)
    }

    ## respect 'mc.cores', overriding env. variables an detectCores()
    result <- getOption("mc.cores", result)

    ## coerce to integer; check for valid value
    tryCatch({
        result <- as.integer(result)
        if ( length(result) != 1L || is.na(result) || result < 1L )
            stop("number of cores must be a non-negative integer")
    }, error = function(e) {
        msg <- paste0(
            conditionMessage(e), ". ",
            "Did you mis-specify R_PARALLELLY_AVAILABLECORES_FALLBACK, ",
            "BIOCPARALLEL_WORKER_NUMBER, or options('mc.cores')?"
        )
        .stop(msg)
    })

    ## override user settings by build-system configurations
    if (identical(Sys.getenv("IS_BIOC_BUILD_MACHINE"), "true"))
        result <- min(result, 4L)

    ## from R-ints.texi
    ## @item _R_CHECK_LIMIT_CORES_
    ## If set, check the usage of too many cores in package @pkg{parallel}.  If
    ## set to @samp{warn} gives a warning, to @samp{false} or @samp{FALSE} the
    ## check is skipped, and any other non-empty value gives an error when more
    ## than 2 children are spawned.
    ## Default: unset (but @samp{TRUE} for CRAN submission checks).
    check_limit_cores <- Sys.getenv("_R_CHECK_LIMIT_CORES_", NA_character_)
    check_limit_cores_is_set <-
        !is.na(check_limit_cores) &&
        !identical(toupper(check_limit_cores), "FALSE")
    if (check_limit_cores_is_set)
        result <- min(result, 2L)

    result
}

.enforceWorkers <-
    function(workers, type = NULL)
{
    ## Ensure that user 'workers' does not exceed hard limits; most-
    ## to least stringent. Usually on build systems

    ## R CMD check limit (though it applies outside check, too...
    check_limit_cores <- Sys.getenv("_R_CHECK_LIMIT_CORES_", NA_character_)
    check_limit_cores_is_set <-
        !is.na(check_limit_cores) &&
        !identical(toupper(check_limit_cores), "FALSE")
    if (workers > 2L && check_limit_cores_is_set) {
        if (!identical(check_limit_cores, "warn")) {
            .stop(
                "_R_CHECK_LIMIT_CORES_' environment variable detected, ",
                "BiocParallel workers must be <= 2 was (", workers, ")"
            )
        }
        .warning(
            "'_R_CHECK_LIMIT_CORES_' environment variable detected, ",
            "setting BiocParallel workers to 2 (was ", workers, ")"
        )
        workers <- 2L
    }

    ## Bioconductor build system
    test <-
        (workers > 4L) && identical(Sys.getenv("IS_BIOC_BUILD_MACHINE"), "true")
    if (test) {
        .warning(
            "'IS_BIOC_BUILD_MACHINE' environment variable detected, ",
            "setting BiocParallel workers to 4 (was ", workers, ")"
        )
        workers <- 4L
    }

    worker_max <- .workerEnvironmentVariable("BIOCPARALLEL_WORKER_MAX")
    if (!is.na(worker_max) && workers > worker_max) {
        .warning(
            "'BIOCPARALLEL_WORKER_MAX' environment variable detected, ",
            "setting BiocParallel workers to ", worker_max, " ",
            "(was ", workers, ")"
        )
        workers <- worker_max
    }

    ## limit on number of available sockets
    if (!is.null(type) && workers > .snowCoresMax(type)) {
        max <- .snowCoresMax(type)
        .warning(
            "worker number limited by available socket connections, ",
            "setting BiocParallel workers to ", max, " (was ", workers, ")"
        )
        workers <- max
    }

    workers
}
