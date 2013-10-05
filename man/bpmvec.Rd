\name{bpmvec}
\alias{bpmvec}
\alias{bpmvec,ANY,ANY-method}
\alias{bpmvec,ANY,BiocParallelParam-method}
\alias{bpmvec,ANY,missing-method}
\alias{bpmvec,ANY,SerialParam-method}

\title{Parallel, vectorized evaluation with multiple vectorized arguments}

\description{

  \code{bpmvec} applies \code{FUN} to subsets of each element of
  \code{\dots}, and then combines the results using \code{AGGREGATE} (as
  in \code{\link{bpvec}}). Each element of \code{\dots} must satisfy the
  same conditions as the \code{X} argument of \code{\link{bpvec}}.

}

\usage{
bpmvec(FUN, ..., MoreArgs=NULL, AGGREGATE=c, BPPARAM)

\S4method{bpmvec}{ANY,ANY}(FUN, ..., MoreArgs=NULL, AGGREGATE=c, BPPARAM)

\S4method{bpmvec}{ANY,missing}(FUN, ..., MoreArgs=NULL, AGGREGATE=c, BPPARAM)

\S4method{bpmvec}{ANY,BiocParallelParam}(FUN, ..., MoreArgs=NULL, AGGREGATE=c, BPPARAM)

\S4method{bpmvec}{ANY,SerialParam}(FUN, ..., MoreArgs=NULL, AGGREGATE=c, BPPARAM)

}

\arguments{

  
  \item{FUN}{The \code{function} to be applied to subsets of the
    elements of \code{\dots}.}

  \item{\dots}{vectorized arguments to \code{FUN}. Each argument will be
    recycled to the length of the longest one and then split into
    pieces, and \code{FUN} will be applied to each matched set of split
    pieces.}
  
  \item{MoreArgs}{Additional (non-vectorized) arguments for
    \code{FUN}. These will not be split, but will be passed whole to
    each parallel invocation of \code{FUN}.}

  \item{AGGREGATE}{See \code{\link{bpvec}}}

  \item{BPPARAM}{A optional \code{\link{BiocParallelParam}} instance
    determining the parallel back-end to be used during evaluation.}

}

\details{

  When \code{BPPARAM} is \code{missing}, \code{bpmvec} uses
  \code{registered()[[1]]}, i.e., the default mechanism for parallel
  evaluation.

  When \code{BPPARAM} is an instance of a class derived from
  \code{BiocParallelParam} for which no other method applies, this
  method creates a vector of indexes and uses these in conjunction with
  \code{bplapply} to arrange for parallel evaluation.

  When \code{BPPARAM} is a class for which no method is defined (e.g.,
  \code{\link{SerialParam}}), \code{FUN} is called directly with no
  parallelization.x

  See \code{showMethods{bpmvec}} for additional methods, e.g.,
  \code{method?bpmvec("MulticoreParam")}.

}

\value{

  This should return the same thing as \code{do.call(FUN, c(list(...),
  MoreArgs))} assuming that \code{AGGREGATE} is set appropriately to
  join the values returned by \code{FUN}.

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
showMethods("bpmvec")

## Parallel addition or two vectors (probably not the best use)
system.time(result <- bpmvec(`+`, 1:10, 10:1))
result

}

\keyword{manip}
