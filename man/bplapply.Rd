\name{bplapply}
\alias{bplapply}
\alias{bplapply,ANY,list-method}
\alias{bplapply,ANY,missing-method}
\alias{bplapply,ANY,BiocParallelParam-method}
\alias{bplapply,ANY,BatchJobsParam-method}
\alias{bplapply,ANY,DoparParam-method}
\alias{bplapply,ANY,SerialParam-method}
\alias{bplapply,ANY,SnowParam-method}

\title{Parallel lapply-like functionality}

\description{

  \code{bplapply} applies \code{FUN} to each element of \code{X}. Any
  type of object \code{X} is allowed, provided \code{length}, \code{[},
  and \code{[[} methods are available. The return value is a \code{list}
  of length equal to \code{X}, as with \code{\link[base]{lapply}}.

}

\usage{

bplapply(X, FUN, ..., BPPARAM=bpparam())

\S4method{bplapply}{ANY,missing}(X, FUN, ..., BPPARAM=bpparam())

\S4method{bplapply}{ANY,BiocParallelParam}(X, FUN, ..., BPPARAM=bpparam())

}

\arguments{

  \item{X}{
    Any object for which methods \code{length}, \code{[}, and 
    \code{[[} are implemented.
  }
  \item{FUN}{
    The \code{function} to be applied to each element of \code{X}.
  }
  \item{\dots}{
    Additional arguments for \code{FUN}, as in \code{\link{lapply}}.
    \itemize{
      \item{BPRESUME}{
        Flag to determine if a previous partially successful run
        should be resumed.  See \code{\link{bpresume}} for details.
        Supported for \code{BatchJobsParam} only.
      }
    }
  }
  \item{BPPARAM}{
    An optional \code{\link{BiocParallelParam}} instance
    determining the parallel back-end to be used during evaluation, or a
    \code{list} of \code{BiocParallelParam} instances, to be applied in
    sequence for nested calls to \code{bplapply}.
  }

}

\details{

  See \code{showMethods{bplapply}} for additional methods, e.g.,
  \code{method?bplapply("MulticoreParam")}.

}

\value{See \code{\link[base]{lapply}}.}

\author{

  Martin Morgan \url{mailto:mtmorgan@fhcrc.org}. Original code as
  attributed in \code{\link[parallel]{mclapply}}.

}

\seealso{

  \itemize{
    \item \code{\link{bpvec}} for parallel, vectorized calculations.

    \item \code{\link{BiocParallelParam}} for possible values of \code{BPPARAM}.
  }
}

\examples{
showMethods("bplapply")

## ten tasks (1:10) so ten calls to FUN default registered parallel
## back-end. Compare with bpvec.
system.time(result <- bplapply(1:10, function(v) {
    message("working") ## 10 tasks
    sqrt(v)
}))
result

}

\keyword{manip}
