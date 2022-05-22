message("Testing DoparParam")

test_DoparParam_orchestration_error <- function() {
    test <-
        requireNamespace("foreach", quietly = TRUE) &&
        requireNamespace("doParallel", quietly = TRUE)
    if (!test)
        DEACTIVATED("'foreach' or 'doParallel' not available")

    if (identical(.Platform$OS.type, "windows"))
        DEACTIVATED("'DoparParam' orchestration error test not run on Windows")

    y <- tryCatch({
        cl <- parallel::makeCluster(1L)
        doParallel::registerDoParallel(cl)
        bplapply(1L, function(x) quit("no"), BPPARAM = DoparParam())
    }, error = function(e) {
        conditionMessage(e)
    }, finally = {
        parallel::stopCluster(cl)
    })
    checkTrue(startsWith(y, "'DoparParam()' foreach() error occurred: "))
}

test_DoparParam_bplapply <- function() {
    test <-
        requireNamespace("foreach", quietly = TRUE) &&
        requireNamespace("doParallel", quietly = TRUE)
    if (!test)
        DEACTIVATED("'foreach' or 'doParallel' not available")

    cl <- parallel::makeCluster(2L)
    on.exit(parallel::stopCluster(cl))
    doParallel::registerDoParallel(cl)
    res0 <- bplapply(1:9, function(x) x + 1L, BPPARAM = SerialParam())
    res <- bplapply(1:9, function(x) x + 1L, BPPARAM = DoparParam())
    checkIdentical(res, res0)
}

test_DoparParam_bplapply_rng <- function() {
    test <-
        requireNamespace("foreach", quietly = TRUE) &&
        requireNamespace("doParallel", quietly = TRUE)
    if (!test)
        DEACTIVATED("'foreach' or 'doParallel' not available")

    cl <- parallel::makeCluster(2L)
    on.exit(parallel::stopCluster(cl))
    doParallel::registerDoParallel(cl)
    res0 <- bplapply(1:9, function(x) runif(1),
                     BPPARAM = SerialParam(RNGseed = 123))
    res <- bplapply(1:9, function(x) runif(1),
                    BPPARAM = DoparParam(RNGseed = 123))
    checkIdentical(res, res0)
}

test_DoparParam_stop_on_error <- function() {
    test <-
        requireNamespace("foreach", quietly = TRUE) &&
        requireNamespace("doParallel", quietly = TRUE)
    if (!test)
        DEACTIVATED("'foreach' or 'doParallel' not available")

    cl <- parallel::makeCluster(2L)
    on.exit(parallel::stopCluster(cl))
    doParallel::registerDoParallel(cl)

    fun <- function(x) {
        if (x == 2) stop()
        x
    }
    res1 <- bptry(bplapply(1:4, fun, BPPARAM = DoparParam(stop.on.error = F)))
    checkEquals(res1[c(1,3,4)], as.list(c(1,3,4)))
    checkTrue(is(res1[[2]], "error"))

    res2 <- bptry(bplapply(1:6, fun, BPPARAM = DoparParam(stop.on.error = T)))
    checkEquals(res2[c(1,4:6)], as.list(c(1,4:6)))
    checkTrue(is(res2[[2]], "error"))
    checkTrue(is(res2[[3]], "error"))
}
