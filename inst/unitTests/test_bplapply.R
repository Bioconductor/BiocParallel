library(doParallel)  ## FIXME: unload?
quiet <- suppressWarnings

test_bplapply_Params <- function()
{
    registerDoParallel(2)
    params <- list(serial=SerialParam(),
                   snow=SnowParam(2),
                   dopar=DoparParam(),
                   batchjobs=BatchJobsParam())
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2)

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
    registerDoParallel(2)
    params <- list(serial=SerialParam(),
                   snow=SnowParam(2),
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
