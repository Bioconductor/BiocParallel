message("Testing bpvectorize")

test_bpvectorize_Params <- function()
{
    cl <- parallel::makeCluster(2)
    doParallel::registerDoParallel(cl)
    params <- list(serial=SerialParam(),
                   snow=SnowParam(2),
                   batchjobs=BatchJobsParam(2, progressbar=FALSE),
                   dopar=DoparParam())
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2)

    x <- 1:10
    expected <- sqrt(x)
    for (param in names(params)) {
        psqrt <- bpvectorize(sqrt, BPPARAM=params[[param]])
        checkIdentical(expected, psqrt(x))
    }

    ## clean up
    foreach::registerDoSEQ()
    parallel::stopCluster(cl)
    closeAllConnections()
    TRUE
}
