test_SnowParam_coerce <- function()
{
    cl <- parallel::makeCluster(2L)
    p <- as(cl, "SnowParam")
    checkTrue(validObject(p))
    checkException(bpstart(p), silent=TRUE)
    checkException(bpstop(p), silent=TRUE)
    parallel::stopCluster(cl)
}
