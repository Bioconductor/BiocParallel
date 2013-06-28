test_BatchJobsParam <- function() {
  if(FALSE) { # getErrors in RUnit conflicts with BatchJobs ...
  backend <- registered()$BatchJobsParam
  register(backend)

  checkEquals(bplapply(1:3, identity), as.list(1:3))
  checkTrue(lastError$isEmpty())
  checkException(bplapply(1:4, function(x) if (x > 3) stop(x) else x))
  checkTrue(!lastError$isEmpty())
  checkEquals(bplapply(lastError, identity), as.list(1:4))
  checkTrue(!lastError$isEmpty())
  }
}
