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
    if (!is.null(err))
      print(as.character(err))
    checkTrue(is.null(err))
    checkIdentical(expected, obs)
}

test_bplapply_Params <- function()
{
    params <- list(serial=SerialParam(),
                   mc=MulticoreParam(2),
                   mclog=MulticoreParam(2, log=TRUE),
                   snow=SnowParam(2, "SOCK"),
                   snowlog=SnowParam(2, "SOCK", log=TRUE),
                   dopar=DoparParam(),
                   batchjobs=BatchJobsParam())
    dop <- registerDoParallel(cores=2)

    x <- 1:10
    expected <- lapply(x, sqrt)
    for (ptype in names(params)) {
        .fork_not_windows(expected,
                          bplapply(x, sqrt, BPPARAM=params[[ptype]]))
    }


    # test empty input
    for (ptype in names(params)) {
      .fork_not_windows(list(),
        bplapply(list(), identity, BPPARAM=params[[ptype]]))
    }

    closeAllConnections()
    TRUE
}

test_bplapply_symbols <- function()
{
    params <- list(serial=SerialParam(),
                   mc=MulticoreParam(2),
                   mclog=MulticoreParam(2, log=TRUE),
                   snow=SnowParam(2, "SOCK"),
                   snowlog=SnowParam(2, "SOCK", log=TRUE),
                   dopar=DoparParam(),
                   batchjobs=BatchJobsParam())
    dop <- registerDoParallel(cores=2)

    X <- list(as.symbol(".XYZ"))
    expected <- lapply(X, as.character)
    for (ptype in names(params)) {
        .fork_not_windows(expected,
                          bplapply(X, as.character, BPPARAM=params[[ptype]]))
    }

    closeAllConnections()
    TRUE
}
