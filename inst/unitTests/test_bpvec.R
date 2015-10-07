library(doParallel)

test_bpvec_Params <- function()
{
    registerDoParallel(2)
    params <- list(serial=SerialParam(),
                   snow=SnowParam(2),
                   batchjobs=BatchJobsParam(workers=2),
                   dopar=DoparParam())
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2)

    x <- rev(1:10) 
    expected <- sqrt(x)
    for (param in names(params)) {
        current <- bpvec(x, sqrt, BPPARAM=params[[param]])
        checkIdentical(current, expected)
    }

    ## clean up
    env <- foreach:::.foreachGlobals
    rm(list=ls(name=env), pos=env)
    closeAllConnections()
    TRUE
}

test_bpvec_MulticoreParam_short_jobs <- function() {
    ## bpvec should return min(length(X), bpworkers())
    if (.Platform$OS.type == "windows")
        return(TRUE)

    exp <- 1:2
    obs <- bpvec(exp, c, AGGREGATE=list, BPPARAM=MulticoreParam(workers=4L))
    checkIdentical(2L, length(obs))
    checkIdentical(exp, unlist(obs))

    ## clean up
    closeAllConnections()
    TRUE
}
