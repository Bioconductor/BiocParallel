test_errorhandling <- function() {
  # FIXME we need the windows workaround
  if (FALSE) {
    library(BiocParallel)
    library(RUnit)
  }

  x = 1:10
  y = rev(x)
  f = function(x, y) if (x > y) stop("whooops") else TRUE

  params <- list(serial=SerialParam(catch.errors=FALSE),
                 snow0=SnowParam(2, "FORK", catch.errors=FALSE),
                 snow1=SnowParam(2, "PSOCK", catch.errors=FALSE),
                 batchjobs=BatchJobsParam(catch.errors=FALSE, progressbar=FALSE),
                 multi=MulticoreParam(catch.errors=FALSE),
                 dopar=DoparParam(catch.errors=FALSE))
  for (param in params) {
    checkException(bpmapply(f, x, y, BPPARAM=param))
  }

  params <- list(serial=SerialParam(catch.errors=TRUE),
                 snow0=SnowParam(2, "FORK", catch.errors=TRUE),
                 snow1=SnowParam(2, "PSOCK", catch.errors=TRUE),
                 batchjobs=BatchJobsParam(catch.errors=TRUE, progressbar=FALSE),
                 multi=MulticoreParam(catch.errors=TRUE),
                 dopar=DoparParam(catch.errors=TRUE))
  for (param in params) {
    checkException(bpmapply(f, x, y, BPPARAM=param))
  }
  # TODO snow -> still no catch?
  # TODO multicore -> convert error message before throwing
  # LastError -> convert all error messages?
  
  TRUE
}
