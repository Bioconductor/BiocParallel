\name{mcmapply}
\alias{mcmapply}
\title{Bioconductor mcmapply, mapply across multiple cores (Unix-alike only)}

\description{
  This is a parallel version of \code{\link{mapply}}, which is used to
  perform parallel calculations over multiple input vectors.
}

\usage{
mcmapply(FUN, ..., MoreArgs = NULL, SIMPLIFY = TRUE, USE.NAMES = TRUE)
}

\arguments{
  All arguments are as in \code{mapply}. In addition, \code{mcmapply}
  supports all the "mc.*" arguments of \code{\link{mclapply}}, which can
  be used to control how the calculation is parallelized.
}

\details{
  See \code{\link{mapply}} for details about the arguments and return value.

  This function uses \code{\link{mclapply}} to implement its
  parallelism, so it will only parallelize when that function is able
  to.
}

\value{See \code{\link{mapply}}.}

\author{
  Ryan Thompson \url{mailto:rct@thompsonclan.org}. Original code as
  attributed in \code{\link[parallel]{mapply}}.
}

\seealso{
  \code{\link{mapply}} for the non-parallel work-alike for this
  function. \code{\link{mclapply}} for parallel calculations across a
  single vector.
}

\examples{
if (.Platform$OS.type != "windows") {
    ## ten tasks (1:10) so ten calls to FUN
    ## compare with pvec
    system.time(result <- mcmapply(1:10, function(v) {
        message("working") ## 10 tasks
        sqrt(v)
    }))
    result
}}

\keyword{manip}
