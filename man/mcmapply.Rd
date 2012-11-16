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
## Examples copied from mapply help
mcmapply(rep, 1:4, 4:1, mc.cores=2)

mcmapply(rep, times = 1:4, x = 4:1)

mcmapply(rep, times = 1:4, MoreArgs = list(x = 42))

mcmapply(function(x,y) seq_len(x) + y,
       c(a =  1, b = 2, c = 3),  # names from first
       c(A = 10, B = 0, C = -10))

word <- function(C,k) paste(rep.int(C,k), collapse = '')
utils::str(mcmapply(word, LETTERS[1:6], 6:1, SIMPLIFY = FALSE))
}}

\keyword{manip}
