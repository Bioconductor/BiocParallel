quiet <- suppressWarnings

test_bplapply_Params <- function()
{
    doParallel::registerDoParallel(2)
    params <- list(serial=SerialParam(),
                   snow=SnowParam(2),
                   dopar=DoparParam(),
                   batchjobs=BatchJobsParam(2, progressbar=FALSE))
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

    # unnamed args for BatchJobs -> dispatches to batchMap
    f <- function(i, x, y, ...) { list(y, i, x) }
    current <- bplapply(2:1, f, c("A", "B"), x=10,
                        BPPARAM=BatchJobsParam(2, progressbar=FALSE))
    checkTrue(all.equal(current[[1]], list(c("A", "B"), 2, 10))) 
    checkTrue(all.equal(current[[2]], list(c("A", "B"), 1, 10))) 

    ## clean up
    env <- foreach:::.foreachGlobals
    rm(list=ls(name=env), pos=env)
    closeAllConnections()
    TRUE
}

test_bplapply_symbols <- function()
{
    doParallel::registerDoParallel(2)
    params <- list(serial=SerialParam(),
                   snow=SnowParam(2),
                   dopar=DoparParam()
                   ## FIXME, batchjobs=BatchJobsParam(2, progressbar=FALSE))
                   )
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2)

    x <- list(as.symbol(".XYZ"))
    expected <- lapply(x, as.character)
    for (param in names(params)) {
        current <- quiet(bplapply(x, as.character, BPPARAM=params[[param]]))
        checkIdentical(expected, current)
    }

    ## clean up
    env <- foreach:::.foreachGlobals
    rm(list=ls(name=env), pos=env)
    closeAllConnections()
    TRUE
}
