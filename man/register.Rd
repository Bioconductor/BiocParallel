\name{register}
\alias{register}
\alias{registered}

\title{Maintain a global registry of available back-end Params}

\description{

  Use functions on this page to add to or query a registry of back-ends,
  including the default for use when no \code{BPPARAM} object is
  provided to functions..

}

\usage{
register(BPPARAM, default=TRUE)
registered(bpparamClass)
}

\arguments{

  \item{BPPARAM}{
    An instance of a \code{BiocParallelParam} class, e.g.,
    \code{\link{MulticoreParam}}, \code{\link{SnowParam}},
    \code{\link{DoparParam}}.
  }

  \item{default}{
    Make this the default \code{BiocParallelParam} for subsequent
    evaluations? If \code{FALSE}, the argument is placed at the lowest
    priority position.
  }

  \item{bpparamClass}{
    When present, the text name of the \code{BiocParallelParam} class
    (e.g., \dQuote{MulticoreParam}) to be retrieved from the
    registry. When absent, a list of all registered instances is
    returned.
  }
}

\details{

  Registering a back-end provides a configuration for parallel
  evaluation. Only one instance of a particular \code{BiocParallelParam}
  class present in the registry. Except when \code{default=FALSE}, the
  most recently registered \code{BiocParallelParam} instance becomes the
  default for subsequent parallel evaluation.

}

\value{

  \code{register} returns, invisibly, a list of registered back-ends.

  \code{registered} returns the back-end of type \code{bpparamClass} or,
  if \code{bpparamClass} is missing,  a list of all registered back-ends.

}

\author{
  Martin Morgan \url{mailto:mtmorgan@fhcrc.org}.
}

\seealso{

  \code{\link{BiocParallelParam}} for possible values of \code{BPPARAM}.

}

\examples{
registered()
}

\keyword{manip}
