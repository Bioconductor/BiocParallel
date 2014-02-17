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
    params <- list(serial=SerialParam(),
                   mc=MulticoreParam(2),
                   snow0=SnowParam(2, "FORK"),
                   snow1=SnowParam(2, "PSOCK"),
                   batchjobs=BatchJobsParam(workers=2),
                   dopar=DoparParam())

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
