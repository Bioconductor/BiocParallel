\name{bpcontrols}
\alias{bpcontrols}

\alias{bpbackend<-}
\alias{bpbackend}
\alias{bpisup}
\alias{bpstart}
\alias{bpstop}
\alias{bpworkers}

\alias{bpbackend,missing-method}
\alias{bpbackend<-,missing,ANY-method}
\alias{bpisup,ANY-method}
\alias{bpisup,missing-method}
\alias{bpstart,ANY-method}
\alias{bpstart,missing-method}
\alias{bpstop,ANY-method}
\alias{bpstop,missing-method}
\alias{bpworkers,missing-method}

\title{Control (start, stop, query) back-end Params}

\description{

  Use functions on this page to start, stop, and query the
  \sQuote{back-ends} that perform a parallel evaluation.

}

\usage{
bpworkers(x, ...)

bpstart(x, ...)
bpstop(x, ...)
bpisup(x, ...)

bpbackend(x, ...)
bpbackend(x, ...) <- value
}

\arguments{
  
  \item{x}{

    An instance of a \code{BiocParallelParam} class, e.g.,
    \code{\link{MulticoreParam}}, \code{\link{SnowParam}},
    \code{\link{DoparParam}}.

    \code{x} can be missing, in which case the default back-end (see
    \code{\link{register}}) is used.

  }

  \item{...}{Additional arguments, perhaps used by methods.}

  \item{value}{Replace value for back-end.}

}

\details{

  \code{bpworkers} reports the number of workers in the back-end as a
  scalar integer with value >= 0.

  \code{bpstart} starts the back-end, if necessary. For instance,
  \code{MulticoreParam} back-ends do not need to be started, but
  \code{SnowParam} back-ends do. \code{bp*} functions like
  \code{bplapply} will automatically start the back-end if necessary.

  \code{bpstop} stops the back-end, if necessary and possible.

  \code{bpisup} tests whether the back-end is available for processing,
  returning a scalar logical value.

  \code{bpbackend} retrieves an object representing the back end, if
  possible. Not all back-ends can be retrieved; see
  \code{showMethods("backend")}.

  \code{bpbackend<-} updates the back end, and is only meant for
  developer use.

}

\value{

  \code{bpworkers} returns a scalar integer >= 0.

  \code{bpisup} returns a scalar logical.

  \code{bpstart}, \code{bpstop} return an updated \code{x},
  invisibly.

  \code{bpbackend}, \code{bpbackend<-} return or accept back
  end-specific objects.

}

\author{
  Martin Morgan \url{mailto:mtmorgan@fhcrc.org}.
}

\seealso{

  \code{\link{BiocParallelParam}} for possible values of \code{x}.

}

\examples{
bpworkers(SerialParam())

\dontrun{
  p <- SnowParam(2L)
  bpworkers(p)          # 2 local nodes, communicating via sockets
  bpstart(p)            # start cluster
  bplapply(1:10, sqrt, BPPARAM=p)
  bpstop(p)             # stop cluster

  p <- SnowParam(4L)
  bplapply(1:10, sqrt, BPPARAM=p)  # automatically start / stop cluster
}  

}

\keyword{manip}
