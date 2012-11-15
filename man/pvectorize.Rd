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
pvectorize(FUN, vectorized.arg=1)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{FUN}{A function whose first argument has a \code{length} and can
    be subset \code{[}, and whose evaluation would benefit by splitting
    the argument into subsets, each one of which is independently
    transformed by \code{FUN}.}
  \item{vectorized.arg}{
    The position or name of the function argument that is vectorized. For
    most functions, the defult of 1 is ok. Note that as with
    \code{pvec}, only a single argument may be vectorized.
  }
}

\details{
  The result of \code{pvectorize} is a function with signature
  \code{\dots}; arguments to the returned function are the original
  arugments \code{FUN}, plus named arguments used by \code{pvec} to
  control how parallel evaluation occurs.

  The only caveat is that the vectorized argument will now be the first
  argument in the function's signature, regardless of the original
  function's argument order. You can avoid this problem by only passing
  named arguments to the function.
}

\value{
  A function taking the same named arguments as \code{FUN}, but evaluated
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

# Contrived example with a vectorized second argument and scalar
# first argument
myfun <- function(offset, v) v + offset[[1]]
myfun(2, 1:50)
pmyfun <- pvectorize(myfun, vectorized.arg="v")
# Argument "v" comes first now
pmyfun(1:50, 2)
# Avoid problems with argument order by using named args
pmyfun(offset=2, v=1:50)

}

\keyword{interface}
