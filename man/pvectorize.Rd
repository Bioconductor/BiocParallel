\name{pvectorize}
\alias{pvectorize}

\title{
  Transform vectorized functions into parallelized, vectorized function
}
\description{
  This transforms a vectorized function into a parallelized version of itself.
}
\usage{
pvectorize(FUN, vectorize.args)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{FUN}{
    A function that is vectorized over at least one of its arguments.
  }
  \item{vectorize.args}{
    Character vector of argument names for which \code{FUN} is
    vectorized. If this is not specified, all arguments are assumed to
    be vectorized.
  }
}

\details{
  \code{pvectorize} wraps \code{FUN} in a new function that parallelizes
  the vectorized calculation performed by \code{FUN} by splitting the
  vectorized arguments into chunks, evaluating \code{FUN} on each set of
  chunks, and then concatenating the results.

  In order to work properly, each vectorized argument of \code{FUN} must
  support \code{length} and subsetting by \code{[}.

  Note that \code{FUN} must already be vectorized. \code{pvectorize}
  will not turn a scalar function into a vectorized one.

  The returned function uses \code{mclapply} to implement parallelism,
  so the function's computations will only be done in parallel if your
  platform supports parallelism through mclapply.
}

\value{
  A parallelized version of \code{FUN}. The returned function accepts
  all the same arguments as \code{FUN} in the same order, but will split
  the vectorized arguments into chunks and evaluate \code{FUN} on each
  set of chunks in parallel. The returned function also accepts all the
  "mc.*" arguments of \code{\link{pvec}}, which can be used to control
  how the calculation is parallelized.
}

\author{
  Ryan Thompson \url{mailto:rct@thompsonclan.org}
}

\seealso{\code{pvec}, \code{mclapply}, \code{Vectorize}}

\examples{

psqrt <- pvectorize(sqrt)
psqrt(1:1000)
all.equal(psqrt(1:1000), sqrt(1:1000))

# Contrived example with a vectorized second argument and scalar
# first argument
myfun <- function(offset, v) v + offset[[1]]
myfun(2, 1:50)
pmyfun <- pvectorize(myfun, vectorize.args="v")
pmyfun(2, 1:50)
all.equal(myfun(2, 1:50), pmyfun(2, 1:50))

# Function with multiple vectorized arguments and one scalar argument
myfun2 <- function(x, y, denom) (x+y)/denom[[1]]
pmyfun2 <- pvectorize(myfun2, vectorize.args=c("x", "y"))
myfun2(1:50, 1:100, 5)
pmyfun2(1:50, 1:100, 5)
all.equal(myfun2(1:50, 1:100, 5), pmyfun2(1:50, 1:100, 5))
}

\keyword{interface}
