\name{bpvec}
\alias{bpvec}
\alias{bpvec,ANY,missing-method}
\alias{bpvec,ANY,MulticoreParam-method}
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

\S4method{bpvec}{ANY,BiocParallelParam}(X, FUN, ..., AGGREGATE=c, 
    BPREDO=list(), BPPARAM=bpparam())

\S4method{bpvec}{ANY,missing}(X, FUN, ..., AGGREGATE=c, 
    BPREDO=list(), BPPARAM=bpparam())
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
  \item{BPPARAM}{A optional \code{\link{BiocParallelParam}} instance
    determining the parallel back-end to be used during evaluation.
  }
  \item{BPREDO}{A \code{list} of output from \code{bpvec} with one or
    more failed elements. When a list is given in \code{BPREDO},
    \code{bpok} is used to identify errors, tasks are rerun and inserted
    into the original results.
  }

}

\details{

  \code{bpvec} divides the elements of \code{X} as evenly as possible over the
  number of workers. The assumption is that \code{FUN} is vectorized and can be
  applied to a group of \code{X} elements, e.g.,
  \code{c(FUN(x[1]), FUN(x[2]))} is equivalent to \code{FUN(x[1:2])}.

  The default behavior of \code{bplapply} also divides \code{X} evenly over the
  workers, however, there is no assumption that \code{FUN} is vectorized; when 
  a group of elements of \code{X} are passed to a worker, FUN is applied to
  each element individually.
  
}

\value{

  The result should be identical to \code{FUN(X, ...)} (assuming that 
  \code{AGGREGATE} is set appropriately).

}

\author{
  Martin Morgan \url{mailto:mtmorgan@fhcrc.org}. Original code as
  attributed in \code{\link[parallel]{pvec}}.
}

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
