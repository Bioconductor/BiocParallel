library(doParallel)  ## FIXME: unload?
registerDoParallel()
quiet <- suppressWarnings

test_bplapply_Params <- function()
{
    params <- list(serial=SerialParam(),
                   snow=SnowParam(),
                   mc=MulticoreParam(),
                   dopar=DoparParam(),
                   batchjobs=BatchJobsParam())

    x <- 1:10
    expected <- lapply(x, sqrt)
    for (param in names(params)) {
        current <- quiet(bplapply(x, sqrt, BPPARAM=params[[param]]))
        checkIdentical(expected, current)
    }

    # test empty input
    for (param in names(params)) {
        current <- quiet(bplapply(list(), identity, BPPARAM=params[[param]]))
        checkIdentical(list(), current)
    }

    closeAllConnections()
    TRUE
}

test_bplapply_symbols <- function()
{
    params <- list(serial=SerialParam(),
                   snow=SnowParam(),
                   dopar=DoparParam())
                  # batchjobs=BatchJobsParam()) ## FIXME
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2)

    x <- list(as.symbol(".XYZ"))
    expected <- lapply(x, as.character)
    for (param in names(params)) {
        current <- quiet(bplapply(x, as.character, BPPARAM=params[[param]]))
        checkIdentical(expected, current)
    }

    closeAllConnections()
    TRUE
}
