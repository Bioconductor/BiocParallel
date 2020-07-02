\name{bpschedule}
\alias{bpschedule}

\alias{bpschedule,missing-method}
\alias{bpschedule,ANY-method}

\title{Schedule back-end Params}

\description{

  Use functions on this page to influence scheduling of parallel
  processing.

}

\usage{
bpschedule(x)
}

\arguments{

  \item{x}{

    An instance of a \code{BiocParallelParam} class, e.g.,
    \code{\link{MulticoreParam}}, \code{\link{SnowParam}},
    \code{\link{DoparParam}}.

    \code{x} can be missing, in which case the default back-end (see
    \code{\link{register}}) is used.

  }

}

\details{

  \code{bpschedule} returns a logical(1) indicating whether the parallel
  evaluation should occur at this point.

}

\value{

  \code{bpschedule} returns a scalar logical.

}

\author{
  Martin Morgan \url{mailto:mtmorgan@fhcrc.org}.
}

\seealso{

  \code{\link{BiocParallelParam}} for possible values of \code{x}.

}

\examples{
bpschedule(SnowParam())                 # TRUE
bpschedule(MulticoreParam(2))           # FALSE on windows

p <- MulticoreParam()
bpschedule(p)                           # TRUE
bplapply(1:2, function(i, p) {
    bpschedule(p)                       # FALSE
}, p = p, BPPARAM=p)
}

\keyword{manip}
