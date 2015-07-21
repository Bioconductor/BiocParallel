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
    registerDoParallel(2)
    params <- list(serial=SerialParam(),
                   snow=SnowParam(2),
                   batchjobs=BatchJobsParam(workers=2),
                   dopar=DoparParam())
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2)
    dop <- registerDoParallel(cores=2)

    x <- 1:10
    expected <- sqrt(x)
    for (ptype in names(params)) {
        psqrt <- bpvectorize(sqrt, BPPARAM=params[[ptype]])
        .fork_not_windows(expected, psqrt(x))
    }

    closeAllConnections()
}
