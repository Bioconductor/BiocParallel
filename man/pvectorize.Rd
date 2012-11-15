\name{pvectorize}
\alias{pvectorize}

\title{
  Transform vectorized functions into parallelized, vectorized function
}
\description{
  This transforms a vectorized function into a parallelized, vectorized
  function.
}
\usage{
pvectorize(FUN)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{FUN}{A function whose first argument has a \code{length} and can
    be subset \code{[}, and whose evaluation would benefit by splitting
    the argument into subsets, each one of which is independently
    transformed by \code{FUN}.}
}

\details{
  The result of \code{pvectorize} is a function with signature
  \code{\dots}; arguments to the returned function are the original
  arugments \code{FUN}, plus named arguments used by \code{pvec} to
  control how parallel evaluation occurs.
}

\value{
  A function taking the same arguments as \code{FUN}, but evaluated
  using \code{\link{pvec}} to parallelize evaluation across available
  cores. 
}

\author{
  Ryan Thompson \url{mailto:rct@thompsonclan.org}
}

\seealso{\code{pvec}}

\examples{
psqrt <- pvectorize(sqrt)
psqrt(1:1000)
}

\keyword{interface}
