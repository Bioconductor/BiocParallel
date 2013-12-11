## TODO: More test coverage of optional args

library(doParallel)

.fork_not_windows <- function(expected, expr)
{
    err <- NULL
    obs <- tryCatch(expr, error=function(e) {
        if (!all(grepl("fork clusters are not supported on Windows",
                       conditionMessage(e))))
            err <<- conditionMessage(e)
        expected
    })
    checkTrue(is.null(err))
    checkIdentical(expected, obs)
}

test_bpvectorize_Params <- function()
{
    params <- list(serial=SerialParam(),
                   mc=MulticoreParam(2),
                   snow0=SnowParam(2, "FORK"),
                   snow1=SnowParam(2, "PSOCK"),
                   batchjobs=BatchJobsParam(workers=2),
                   dopar=DoparParam())

    dop <- registerDoParallel(cores=2)

    x <- 10:1
    expected <- sqrt(x)
    for (ptype in names(params)) {
        psqrt <- bpvectorize(sqrt, BPPARAM=params[[ptype]])
        .fork_not_windows(expected, psqrt(x))
    }
}

test_bpvectorize_multi_argument <- function ()
{
    params <- list(serial=SerialParam(),
                   mc=MulticoreParam(2),
                   snow0=SnowParam(2, "FORK"),
                   snow1=SnowParam(2, "PSOCK"),
                   batchjobs=BatchJobsParam(workers=2),
                   dopar=DoparParam())

    dop <- registerDoParallel(cores=2)

    ## Test vectorizing just two args of a multi-arg function
    testfunc <- function(x, y, z) {
        ## C has to be a scalar
        stopifnot(length(z) == 1)
        x + y + z
    }

    x <- 1:10
    y <- 1:5
    z <- 5

    expected <- testfunc(x, y, z)

    for (ptype in names(params)) {
        pvtestfunc <- bpvectorize(testfunc,
                                  VECTOR.ARGS=c("x", "y"),
                                  BPPARAM=params[[ptype]])
        .fork_not_windows(expected, pvtestfunc(x, y, z))
    }
}

test_bpvectorize_all_argument <- function ()
{
    params <- list(serial=SerialParam(),
                   mc=MulticoreParam(2),
                   snow0=SnowParam(2, "FORK"),
                   snow1=SnowParam(2, "PSOCK"),
                   batchjobs=BatchJobsParam(workers=2),
                   dopar=DoparParam())

    dop <- registerDoParallel(cores=2)

    ## Test vectorizing all args with NA
    testfunc <- function(x, y, z) {
        x + y + z
    }

    x <- 1:10
    y <- 1:5
    z <- 1:2

    expected <- testfunc(x, y, z)

    for (ptype in names(params)) {
        pvtestfunc <- bpvectorize(testfunc,
                                  VECTOR.ARGS=NA,
                                  BPPARAM=params[[ptype]])
        .fork_not_windows(expected, pvtestfunc(x, y, z))
    }
}


test_bpvectorize_first_arg_only <- function ()
{
    params <- list(serial=SerialParam(),
                   mc=MulticoreParam(2),
                   snow0=SnowParam(2, "FORK"),
                   snow1=SnowParam(2, "PSOCK"),
                   batchjobs=BatchJobsParam(workers=2),
                   dopar=DoparParam())

    dop <- registerDoParallel(cores=2)

    ## Test vectorizing just the first arg of a multi-arg function,
    ## which should be the default.
    testfunc <- function(x, y, z) {
        ## C has to be a scalar
        stopifnot(length(y) == 1)
        stopifnot(length(z) == 1)
        x + y + z
    }

    x <- 1:10
    y <- 1
    z <- 5

    expected <- testfunc(x, y, z)

    for (ptype in names(params)) {
        pvtestfunc <- bpvectorize(testfunc,
                                  BPPARAM=params[[ptype]])
        .fork_not_windows(expected, pvtestfunc(x, y, z))
    }
}
