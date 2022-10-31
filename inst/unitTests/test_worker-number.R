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
    o_bbs_home <- Sys.getenv("BBS_HOME", NA)
    Sys.unsetenv("BBS_HOME")
    o_worker_n <- Sys.getenv("BIOCPARALLEL_WORKER_NUMBER", NA)
    Sys.unsetenv("BIOCPARALLEL_WORKER_NUMBER")
    on.exit({
        .resetEnv("_R_CHECK_LIMIT_CORES_", o_check_limits)
        .resetEnv("BBS_HOME", o_bbs_home)
        .resetEnv("BIOCPARALLEL_WORKER_NUMBER", o_worker_n)
    })

    checkIdentical(parallel::detectCores() - 2L, bpnworkers(SnowParam()))
    Sys.setenv(BIOCPARALLEL_WORKER_NUMBER = 5)
    checkIdentical(5L, bpnworkers(SnowParam()))
    Sys.setenv(BBS_HOME="foo")
    checkIdentical(4L, bpnworkers(SnowParam()))
    Sys.setenv(`_R_CHECK_LIMIT_CORES_` = TRUE)
    checkIdentical(2L, bpnworkers(SnowParam()))
}

test_enforceWorkers <- function()
{
    o_check_limits <- Sys.getenv("_R_CHECK_LIMIT_CORES_", NA)
    Sys.unsetenv("_R_CHECK_LIMIT_CORES_")
    o_bbs_home <- Sys.getenv("BBS_HOME", NA)
    Sys.unsetenv("BBS_HOME")
    o_worker_max <- Sys.getenv("BIOCPARALLEL_WORKER_MAX", NA)
    Sys.unsetenv("BIOCPARALLEL_WORKER_MAX")
    on.exit({
        .resetEnv("_R_CHECK_LIMIT_CORES_", o_check_limits)
        .resetEnv("BBS_HOME", o_bbs_home)
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

    Sys.setenv(BBS_HOME = "foo")
    warn <- FALSE
    withCallingHandlers({
        obs <- bpnworkers(SnowParam(6))
    }, warning = function(x) {
            warn <<- startsWith(
            trimws(conditionMessage(x)),
            "'BBS_HOME' environment variable detected"
        )
        invokeRestart("muffleWarning")
    })
    checkIdentical(4L, obs)
    checkTrue(warn)
    .resetEnv("BBS_HOME", o_bbs_home)

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
        obs <- bpnworkers(SnowParam(6))
    }, warning = function(x) {
        warn <<- TRUE
        invokeRestart("muffleWarning")
    })
    checkIdentical(6L, obs)
    checkTrue(!warn)

    Sys.setenv(`_R_CHECK_LIMIT_CORES_` = "true")
    checkException(SnowParam(6), silent = TRUE)
    .resetEnv("_R_CHECK_LIMIT_CORES_", o_check_limits)
}
