test_BatchJobsParam <- 
    function() 
{
    param <- BatchJobsParam(progressbar=FALSE, cleanup=TRUE)
    f <- function(x) if (x == 0) stop(x) else x
    checkEquals(bplapply(1:3, f, BPPARAM=param), as.list(1:3))

    param <- BatchJobsParam(progressbar=FALSE, catch.errors=TRUE)
    res <- bplapply(0:3, f, BPPARAM=param)
    checkTrue(inherits(res[[1]], "condition"))
    checkEquals(bplapply(0:3, identity, BPPARAM=param), as.list(0:3))
}
