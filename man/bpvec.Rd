\name{bpvec}
\alias{bpvec}
\alias{bpvec,ANY,ANY,ANY-method}
\alias{bpvec,ANY,ANY,BiocParallelParam-method}
\alias{bpvec,ANY,ANY,missing-method}

\title{Parallel, vectorized evaluation}

\description{

  \code{bpvec} applies \code{FUN} to subsets of \code{X}. Any type of
  object \code{X} is allowed, provided \code{length}, \code{[}, and
  \code{c} methods are available. The return value is a \code{vector} of
  length equal to \code{X}, as with \code{FUN(X)}.

}

\usage{
bpvec(X, FUN, ..., BPPARAM)

\S4method{bpvec}{ANY,ANY,ANY}(X, FUN, ..., BPPARAM)

\S4method{bpvec}{ANY,ANY,BiocParallelParam}(X, FUN, ..., BPPARAM)

\S4method{bpvec}{ANY,ANY,missing}(X, FUN, ..., BPPARAM)
}

\arguments{
  \item{X}{
    Any object for which methods \code{length}, \code{[}, and \code{c}
    are implemented.
  }

  \item{FUN}{The \code{function} to be applied to subsets of \code{X}.}

  \item{\dots}{Additional arguments for \code{FUN}.}

  \item{BPPARAM}{A optional \code{\link{BiocParallelParam}} instance
    determining the parallel back-end to be used during evaluation.}

}

\details{

  When \code{BPPARAM} is \code{missing}, \code{bpvec} uses
  \code{registered()[[1]]}, i.e., the default mechanism for parallel
  evaluation.

  When \code{BPPARAM} is an instance of a class derived from
  \code{BiocParallelParam} for which no other method applies, this
  method creates a vector of indicies and uses these in conjuction with
  \code{bplapply} to arrange for parallelized evaluation.

  When \code{BPPARAM} is a class for which no method is defined (e.g.,
  \code{\link{SerialParam}}), \code{FUN(X)} is used.

  See \code{showMethods{bpvec}} for additional methods, e.g.,
  \code{method?bpvec("MulticoreParam")}.

}

\value{

  The result of \code{c(FUN(X[i0]), FUN(X[i1]), ...)} where \code{X[i0]}
  etc., are sequential subsets of \code{X}.

}

\author{
  Martin Morgan \url{mailto:mtmorgan@fhcrc.org}. Original code as
  attributed in \code{\link[parallel]{pvec}}.
}

\seealso{

  \code{\link{bplapply}} for parallel lapply.

  \code{\link{BiocParallelParam}} for possible values of \code{BPPARAM}.

}

\examples{
showMethods("bpvec")

## ten tasks (1:10), called with as many back-end elements are specified
## by BPPARAM.  Compare with bplapply
system.time(result <- bpvec(1:10, function(v) {
    message("working") ## 10 tasks
    sqrt(v)
}))
result

}

\keyword{manip}
