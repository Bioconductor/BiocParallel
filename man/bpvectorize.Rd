\name{bpvectorize}
\alias{bpvectorize}
\alias{bpvectorize,ANY,ANY-method}
\alias{bpvectorize,ANY,missing-method}
\alias{bpvectorize,ANY,BiocParallelParam-method}

\title{Transform a vectorized function into parallelized, vectorized function}

\description{

  This transforms a vectorized function into a parallel, vectorized
  function. Any function \code{FUN} can be used, provided its
  parallelized argument(s) (by default, the first argument) have
  \code{length} and \code{[} methods defined, and the return value of
  \code{FUN} can be concatenated with \code{c}.

}

\usage{
bpvectorize(FUN, VECTOR.ARGS, ..., AGGREGATE=c, BPPARAM)

\S4method{bpvectorize}{ANY,ANY}(FUN, VECTOR.ARGS, ..., AGGREGATE=c, BPPARAM)

\S4method{bpvectorize}{ANY,missing}(FUN, VECTOR.ARGS, ..., AGGREGATE=c, BPPARAM)

\S4method{bpvectorize}{ANY,BiocParallelParam}(FUN, VECTOR.ARGS, ..., AGGREGATE=c, BPPARAM)

}

\arguments{

  \item{FUN}{A function with at least one argument that is treated in a
    vectorized fashion. See \code{VECTOR.ARGS} for how to specify which
    argument(s) are vectorized. The return value of \code{FUN} must
    support concatenation with \code{c} in order to reassemble the final
    result.}

  \item{VECTOR.ARGS}{A character vector giving the argument names of
    \code{FUN} that represent vectorized arguments. These arguments will
    be recycled to the length of the longest one and then split into
    pieces, and \code{FUN} will be applied to each matched set of split
    pieces.

    These arguments will be split into
    independently-processed chunks, while all other arguments will be
    passed intact. Each vector argument of \code{FUN} named in this
    option should take a value whose first argument has a \code{length}
    and can be subset with \code{[}, and whose evaluation would benefit
    by splitting the argument into subsets, each one of which is
    independently transformed by \code{FUN}. The default is to vectorize
    the first argument of \code{FUN}.}

  \item{...}{Additional arguments to parallization, unused.}

  \item{AGGREGATE}{See \code{\link{bpvec}}.}

  \item{BPPARAM}{An optional \code{\link{BiocParallelParam}} instance
    determining the parallel back-end to be used during evaluation.}

}

\details{

  The result of \code{bpvectorize} is a function with the same signature
  as \code{FUN}, but which splits and parallelizes the calculation using
  \code{BPPARAM}.

  When \code{BPPARAM} is \code{missing}, \code{bpvectorize} uses
  \code{registered()[[1]]}, i.e., the default mechanism for parallel
  evaluation. Note that this is resolved at the time \code{bpvectorize}
  is called and will not change later if the default mechanism is
  changed.

  When \code{BPPARAM} is a class for which no method is defined (e.g.,
  \code{\link{SerialParam}}), \code{FUN} is returned unchanged.

  See \code{showMethods{bpvectorize}} for additional methods, if any.

}

\value{

  A function taking the same arguments as \code{FUN}, but evaluated
  using \code{\link{bpmvec}} for parallel evaluation.

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
