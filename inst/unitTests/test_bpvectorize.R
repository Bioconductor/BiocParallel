library(doParallel)

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
        checkIdentical(expected, psqrt(x))
    }

    closeAllConnections()
}
