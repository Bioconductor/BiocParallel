test_BatchJobsParam <- function() {
  backend <- BatchJobsParam(progressbar = FALSE, cleanup = TRUE)
  register(backend)

  f = function(x) if (x == 0) stop(x) else x
  checkEquals(bplapply(1:3, f), as.list(1:3))
  checkException(bplapply(0:3, f))
  checkEquals(bplapply(LastError, identity), as.list(0:3))
}
