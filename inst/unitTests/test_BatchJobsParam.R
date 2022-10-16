message("Testing BatchJobsParam")

test_BatchJobsParam <-
    function()
{
    param <- BatchJobsParam(2, progressbar=FALSE, cleanup=TRUE)
    checkEquals(as.list(sqrt(1:3)), bplapply(1:3, sqrt, BPPARAM=param))

    X <- list(1, "2", 3)
    param <- BatchJobsParam(2, progressbar=FALSE, stop.on.error=FALSE)
    res <- tryCatch({
        bplapply(X, sqrt, BPPARAM=param)
    }, error=identity)
    checkTrue(is(res, "bplist_error"))
    checkTrue(is(bpresult(res)[[2]], "remote_error"))
}
