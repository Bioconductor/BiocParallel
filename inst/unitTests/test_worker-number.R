## .workerEnvironmentVariable
## .defaultWorkers()
## .enforceWorkers(workers, type)

message("Testing worker-number")

.resetEnv <- function(name, value) {
    if (is.na(value)) {
        Sys.unsetenv(name)
    } else {
        value <- list(value)
        names(value) <- name
        do.call("Sys.setenv", value)
    }
}

test_defaultWorkers <- function()
{
    o_check_limits <- Sys.getenv("_R_CHECK_LIMIT_CORES_", NA)
    Sys.unsetenv("_R_CHECK_LIMIT_CORES_")
    o_bbs_home <- Sys.getenv("IS_BIOC_BUILD_MACHINE", NA)
    Sys.unsetenv("IS_BIOC_BUILD_MACHINE")
    o_worker_n <- Sys.getenv("BIOCPARALLEL_WORKER_NUMBER", NA)
    Sys.unsetenv("BIOCPARALLEL_WORKER_NUMBER")
    on.exit({
        .resetEnv("_R_CHECK_LIMIT_CORES_", o_check_limits)
        .resetEnv("IS_BIOC_BUILD_MACHINE", o_bbs_home)
        .resetEnv("BIOCPARALLEL_WORKER_NUMBER", o_worker_n)
    })

    checkIdentical(parallel::detectCores() - 2L, bpnworkers(SnowParam()))
    Sys.setenv(BIOCPARALLEL_WORKER_NUMBER = 5)
    checkIdentical(5L, bpnworkers(SnowParam()))
    Sys.setenv(IS_BIOC_BUILD_MACHINE="true")
    checkIdentical(4L, bpnworkers(SnowParam()))
    Sys.setenv(`_R_CHECK_LIMIT_CORES_` = TRUE)
    checkIdentical(2L, bpnworkers(SnowParam()))
}

test_enforceWorkers <- function()
{
    o_check_limits <- Sys.getenv("_R_CHECK_LIMIT_CORES_", NA)
    Sys.unsetenv("_R_CHECK_LIMIT_CORES_")
    o_bbs_home <- Sys.getenv("IS_BIOC_BUILD_MACHINE", NA)
    Sys.unsetenv("IS_BIOC_BUILD_MACHINE")
    o_worker_max <- Sys.getenv("BIOCPARALLEL_WORKER_MAX", NA)
    Sys.unsetenv("BIOCPARALLEL_WORKER_MAX")
    on.exit({
        .resetEnv("_R_CHECK_LIMIT_CORES_", o_check_limits)
        .resetEnv("IS_BIOC_BUILD_MACHINE", o_bbs_home)
        .resetEnv("BIOCPARALLEL_WORKER_MAX", o_worker_max)
    })

    checkIdentical(6L, bpnworkers(SnowParam(6L)))
    Sys.setenv(BIOCPARALLEL_WORKER_MAX = 5L)
    warn <- FALSE
    withCallingHandlers({
        obs <- bpnworkers(SnowParam(6))
    }, warning = function(x) {
            warn <<- startsWith(
            trimws(conditionMessage(x)),
            "'BIOCPARALLEL_WORKER_MAX' environment variable detected"
        )
        invokeRestart("muffleWarning")
    })
    checkIdentical(5L, obs)
    checkTrue(warn)
    .resetEnv("BIOCPARALLEL_WORKER_MAX", o_worker_max)

    Sys.setenv(IS_BIOC_BUILD_MACHINE = "true")
    warn <- FALSE
    withCallingHandlers({
        obs <- bpnworkers(SnowParam(6))
    }, warning = function(x) {
            warn <<- startsWith(
            trimws(conditionMessage(x)),
            "'IS_BIOC_BUILD_MACHINE' environment variable detected"
        )
        invokeRestart("muffleWarning")
    })
    checkIdentical(4L, obs)
    checkTrue(warn)
    ## .resetEnv("IS_BIOC_BUILD_MACHINE", o_bbs_home)

    Sys.setenv(`_R_CHECK_LIMIT_CORES_` = "warn")
    warn <- FALSE
    withCallingHandlers({
        obs <- bpnworkers(SnowParam(6))
    }, warning = function(x) {
            warn <<- startsWith(
            trimws(conditionMessage(x)),
            "'_R_CHECK_LIMIT_CORES_' environment variable detected"
        )
        invokeRestart("muffleWarning")
    })
    checkIdentical(2L, obs)
    checkTrue(warn)

    Sys.setenv(`_R_CHECK_LIMIT_CORES_` = "false")
    warn <- FALSE
    withCallingHandlers({
        obs <- bpnworkers(SnowParam(4))
    }, warning = function(x) {
        warn <<- TRUE
        invokeRestart("muffleWarning")
    })
    checkIdentical(4L, obs)
    checkTrue(!warn)

    Sys.setenv(`_R_CHECK_LIMIT_CORES_` = "true")
    checkException(SnowParam(4), silent = TRUE)
}

test_bpnworkers_integer_valued <- function()
{
    ## https://github.com/Bioconductor/BiocParallel/issues/232
    checkTrue(inherits(snowWorkers(), "integer")) # default
    checkIdentical(2L, bpnworkers(SnowParam(c("foo", "bar"))))
    checkIdentical(2L, bpnworkers(SnowParam(2)))
    checkIdentical(2L, bpnworkers(SnowParam(2.1)))
    checkIdentical(2L, bpnworkers(SnowParam(2.9)))

    p <- SnowParam(2); bpworkers(p) <- 2
    checkIdentical(2L, bpnworkers(p))
    bpworkers(p) <- c("foo", "bar")
    checkIdentical(2L, bpnworkers(p))

    if (!identical(.Platform$OS.type, "windows")) {
        checkIdentical(2L, bpnworkers(MulticoreParam(2.1)))
        checkIdentical(2L, bpnworkers(MulticoreParam(2.9)))
        checkIdentical(2L, bpnworkers(MulticoreParam(2)))
        p <- MulticoreParam(2); bpworkers(p) <- 2
        checkIdentical(2L, bpnworkers(p))
    }
}
