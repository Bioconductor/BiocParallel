\name{bpvec}
\alias{bpvec}
\alias{bpvec,ANY,missing-method}
\alias{bpvec,ANY,list-method}
\alias{bpvec,ANY,BiocParallelParam-method}

\title{Parallel, vectorized evaluation}

\description{
  \code{bpvec} applies \code{FUN} to subsets of \code{X}. Any type of
  object \code{X} is allowed, provided \code{length}, and \code{[} are
  defined on \code{X}. \code{FUN} is a function such that
  \code{length(FUN(X)) == length(X)}. The objects returned by \code{FUN}
  are concatenated by \code{AGGREGATE} (\code{c()} by default). The
  return value is \code{FUN(X)}.
}

\usage{
bpvec(X, FUN, ..., AGGREGATE=c, BPREDO=list(), BPPARAM=bpparam(), BPOPTIONS = bpoptions())
}

\arguments{

  \item{X}{
    Any object for which methods \code{length} and \code{[} are
    implemented.
  }

  \item{FUN}{
    A function to be applied to subsets of \code{X}. The relationship
    between \code{X} and \code{FUN(X)} is 1:1, so that
    \code{length(FUN(X, ...)) == length(X)}. The return value of
    separate calls to \code{FUN} are concatenated with \code{AGGREGATE}.
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
  \item{BPOPTIONS}{
    Additional options to control the behavior of the parallel evaluation, see \code{\link{bpoptions}}.
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
  \code{?pvec} man page should be consulted for further details.

}

\value{

  The result should be identical to \code{FUN(X, ...)} (assuming that
  \code{AGGREGATE} is set appropriately).

  When evaluation of individual elements of \code{X} results in an
  error, the result is a \code{list} with the same geometry (i.e.,
  \code{lengths()}) as the split applied to \code{X} to create chunks
  for parallel evaluation; one or more elements of the list contain a
  \code{bperror} element, indicting that the vectorized calculation
  failed for at least one of the index values in that chunk.

  An error is also signaled when \code{FUN(X)} does not return an
  object of the same length as \code{X}; this condition is only detected
  when the number of elements in \code{X} is greater than the number of
  workers.

}

\author{Martin Morgan \url{mailto:mtmorgan@fhcrc.org}.}

\seealso{

  \code{\link{bplapply}} for parallel lapply.

  \code{\link{BiocParallelParam}} for possible values of \code{BPPARAM}.

  \code{\link{pvec}} for background.

}

\examples{
methods("bpvec")

## ten tasks (1:10), called with as many back-end elements are specified
## by BPPARAM.  Compare with bplapply
fun <- function(v) {
    message("working")
    sqrt(v)
}
system.time(result <- bpvec(1:10, fun))
result

## invalid FUN -- length(class(X)) is not equal to length(X)
bptry(bpvec(1:2, class, BPPARAM=SerialParam()))
}

\keyword{manip}
