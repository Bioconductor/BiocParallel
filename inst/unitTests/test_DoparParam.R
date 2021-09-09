test_DoparParam_orchestration_error <- function() {
    test <-
        requireNamespace("foreach", quietly = TRUE) &&
        requireNamespace("doParallel", quietly = TRUE)
    if (!test)
        DEACTIVATED("'foreach' or 'doParallel' not available")

    if (identical(.Platform$OS.type, "windows"))
        DEACTIVATED("'DoparParam' orchestration error test not run on Windows")

    old_warn <- options(warn = 2)
    on.exit(options(old_warn))
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
