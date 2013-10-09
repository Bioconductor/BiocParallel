test_SnowParam_coerce <- 
    function()
{
    cl <- parallel::makeCluster(2L)
    p <- as(cl, "SnowParam")
    checkTrue(validObject(p))

    obs <- tryCatch(bpstart(p), error=conditionMessage)
    exp <- "'bpstart' not available; instance from outside BiocParallel?"
    checkIdentical(exp, obs)

    obs <- tryCatch(bpstop(p), error=conditionMessage)
    exp <- "'bpstop' not available; instance from outside BiocParallel?"
    checkIdentical(exp, obs)

    parallel::stopCluster(cl)
}
