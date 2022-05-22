message("Testing bpvec")

test_bpvec_Params <- function()
{
    cl <- parallel::makeCluster(2)
    doParallel::registerDoParallel(cl)
    params <- list(serial=SerialParam(),
                   snow=SnowParam(2),
                   batchjobs=BatchJobsParam(2, progressbar=FALSE),
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
    foreach::registerDoSEQ()
    parallel::stopCluster(cl)
    closeAllConnections()
    TRUE
}

test_bpvec_MulticoreParam_short_jobs <- function() {
    ## bpvec should return min(length(X), bpnworkers())
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

test_bpvec_invalid_FUN <- function() {
    res <- bptry(bpvec(1:2, class, BPPARAM=SerialParam()))
    checkTrue(inherits(res, "bpvec_error"))
}

test_bpvec_named_list <- function() {
    X <- list()
    Y <- character()
    checkIdentical(X, bpvec(X, length))
    checkIdentical(X, bpvec(Y, length))

    names(X) <- names(Y) <- character()
    checkIdentical(X, bpvec(X, length))
    checkIdentical(X, bpvec(Y, length))
}
