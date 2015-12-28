\name{bpvec}
\alias{bpvec}
\alias{bpvec,ANY,missing-method}
\alias{bpvec,ANY,list-method}
\alias{bpvec,ANY,BiocParallelParam-method}

\title{Parallel, vectorized evaluation}

\description{

  \code{bpvec} applies \code{FUN} to subsets of \code{X}. Any type of
  object \code{X} is allowed, provided \code{length}, \code{[}, and
  \code{c} methods are available. The return value is a \code{vector} of
  length equal to \code{X}, as with \code{FUN(X)}.

}

\usage{
bpvec(X, FUN, ..., AGGREGATE=c, BPREDO=list(), BPPARAM=bpparam())
}

\arguments{
  \item{X}{
    Any object for which methods \code{length}, \code{[}, and \code{c}
    are implemented.
  }
  \item{FUN}{The \code{function} to be applied to subsets of \code{X}.
  }
  \item{\dots}{Additional arguments for \code{FUN}.
  }
  \item{AGGREGATE}{A function taking any number of arguments \code{...}
    called to reduce results (elements of the \code{...} argument of
    \code{AGGREGATE} from parallel jobs. The default, \code{c},
    concatenates objects and is appropriate for vectors; \code{rbind}
    might be appropriate for data frames.
  }
  \item{BPPARAM}{
    An optional \code{\link{BiocParallelParam}} instance
    determining the parallel back-end to be used during evaluation, or a
    \code{list} of \code{BiocParallelParam} instances, to be applied in
    sequence for nested calls to \pkg{BiocParallel} functions.
  }
  \item{BPREDO}{A \code{list} of output from \code{bpvec} with one or
    more failed elements. When a list is given in \code{BPREDO},
    \code{bpok} is used to identify errors, tasks are rerun and inserted
    into the original results.
  }

}

\details{

  This method creates a vector of indices for \code{X} that divide the
  elements as evenly as possible given the number of \code{bpworkers()}
  and \code{bptasks()} of \code{BPPARAM}. Indices and data are passed to
  \code{bplapply} for parallel evaluation.

  The distinction between \code{bpvec} and \code{bplapply} is that
  \code{bplapply} applies \code{FUN} to each element of \code{X}
  separately whereas \code{bpvec} assumes the function is vectorized,
  e.g., \code{c(FUN(x[1]), FUN(x[2]))} is equivalent to
  \code{FUN(x[1:2])}. This approach can be more efficient than
  \code{bplapply} but requires the assumption that \code{FUN} takes a
  vector input and creates a vector output of the same length as the
  input which does not depend on partitioning of the vector. This
  behavior is consistent with \code{parallel:::pvec} and the
  ?\code{pvec} man page should be consulted for further details.

}

\value{

  The result should be identical to \code{FUN(X, ...)} (assuming that 
  \code{AGGREGATE} is set appropriately).

  On error, a \code{list} with the same geometry (i.e.,
  \code{lengths()}) as the split appliedt to \code{X} to create chunks
  for parallel evaluation; elements of the list may contain a
  \code{bperror} element, indicting that the vectorized calculation
  failed for at least one of the index values in that chunk.

}

\author{Martin Morgan \url{mailto:mtmorgan@fhcrc.org}.}

\seealso{

  \code{\link{bplapply}} for parallel lapply.

  \code{\link{BiocParallelParam}} for possible values of \code{BPPARAM}.

  \code{\link{pvec}} for background.

}

\examples{
showMethods("bpvec")

## ten tasks (1:10), called with as many back-end elements are specified
## by BPPARAM.  Compare with bplapply
fun <- function(v) {
    message("working")
    sqrt(v)
}
system.time(result <- bpvec(1:10, fun)) 
result

}

\keyword{manip}
