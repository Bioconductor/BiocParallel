\name{bpvectorize}
\alias{bpvectorize}
\alias{bpvectorize,ANY,ANY-method}
\alias{bpvectorize,ANY,missing-method}

\title{Transform vectorized functions into parallelized, vectorized function}

\description{

  This transforms a vectorized function into a parallel, vectorized
  function. Any function \code{FUN} can be used, provided its
  parallelized argument (by default, the first argument) has a
  \code{length} and \code{[} method defined, and the return value of
  \code{FUN} can be concatenated with \code{c}.

}

\usage{
bpvectorize(FUN, ..., BPREDO=list(), BPPARAM=bpparam(), BPOPTIONS = bpoptions())

\S4method{bpvectorize}{ANY,ANY}(FUN, ..., BPREDO=list(), BPPARAM=bpparam(), BPOPTIONS = bpoptions())

\S4method{bpvectorize}{ANY,missing}(FUN, ..., BPREDO=list(),
    BPPARAM=bpparam(), BPOPTIONS = bpoptions())

}

\arguments{

  \item{FUN}{A function whose first argument has a \code{length} and can
    be subset \code{[}, and whose evaluation would benefit by splitting
    the argument into subsets, each one of which is independently
    transformed by \code{FUN}. The return value of \code{FUN} must
    support concatenation with \code{c}.
  }
  \item{...}{Additional arguments to parallization, unused.
  }
  \item{BPPARAM}{An optional \code{\link{BiocParallelParam}} instance
    determining the parallel back-end to be used during evaluation.
  }
  \item{BPREDO}{A \code{list} of output from \code{bpvectorize} with one
    or more failed elements. When a list is given in \code{BPREDO},
    \code{bpok} is used to identify errors, tasks are rerun and inserted
    into the original results.
  }

  \item{BPOPTIONS}{
    Additional options to control the behavior of the parallel evaluation, see \code{\link{bpoptions}}.
  }
}

\details{

  The result of \code{bpvectorize} is a function with signature
  \code{\dots}; arguments to the returned function are the original
  arguments \code{FUN}. \code{BPPARAM} is used for parallel evaluation.

  When \code{BPPARAM} is a class for which no method is defined (e.g.,
  \code{\link{SerialParam}}), \code{FUN(X)} is used.

  See \code{methods{bpvectorize}} for additional methods, if any.

}

\value{

  A function taking the same arguments as \code{FUN}, but evaluated
  using \code{\link{bpvec}} for parallel evaluation across available
  cores.

}

\author{
  Ryan Thompson \url{mailto:rct@thompsonclan.org}
}

\seealso{\code{bpvec}}

\examples{
psqrt <- bpvectorize(sqrt) ## default parallelization
psqrt(1:10)
}

\keyword{interface}
