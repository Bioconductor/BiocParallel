library(doParallel)                     # FIXME: unload?

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

test_bpvec_Params <- function()
{
    registerDoParallel(2)
    params <- list(serial=SerialParam(),
                   snow=SnowParam(2),
                   batchjobs=BatchJobsParam(workers=2),
                   dopar=DoparParam())
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2)

    dop <- registerDoParallel(cores=2)
    ## FIXME: restore previously registered back-end?

    x <- 1:10
    expected <- sqrt(x)
    for (ptype in names(params)) {
        .fork_not_windows(expected,
                          bpvec(x, sqrt, BPPARAM=params[[ptype]]))
    }

    closeAllConnections()
}

test_bpvec_MulticoreParam_short_jobs <- function() {
    ## bpvec should return min(length(X), bpworkers())
    if (.Platform$OS.type == "windows")
        return(TRUE)
    exp <- 1:2
    obs <- bpvec(exp, c, AGGREGATE=list, BPPARAM=MulticoreParam(workers=4L))
    checkIdentical(2L, length(obs))
    checkIdentical(exp, unlist(obs))
}
