\name{DoparParam-class}
\Rdversion{1.1}
\docType{class}

\alias{DoparParam-class}
\alias{DoparParam}
\alias{coerce,SOCKcluster,DoparParam-method}

\alias{bpbackend,DoparParam-method}
\alias{bpbackend<-,DoparParam,SOCKcluster-method}

\alias{bpisup,DoparParam-method}
\alias{bpstart,DoparParam-method}
\alias{bpstop,DoparParam-method}
\alias{bpworkers,DoparParam-method}

\alias{bplapply,ANY,DoparParam-method}

\alias{show,DoparParam-method}

\title{Enable parallel evaluation using registered dopar backend}

\description{

  This class is used to dispatch parallel operations to the dopar
  backend registerd with the foreach package. TODO LINK

}

\usage{
DoparParam() 

\S4method{bplapply}{ANY,DoparParam}(X, FUN, ..., BPPARAM)
}

\arguments{

  \item{X}{A vector-like object supporting \code{length}, \code{[},
    and, for \code{bplapply}, \code{[[}.}

  \item{FUN}{A function operating on \code{X} and other arguments in
    \code{...}.}

  \item{...}{Additional arguments to \code{FUN}}

  \item{BPPARAM}{An \code{DoparParam}-class instance.}

}

\section{DoparParam constructor}{

  Return a proxy object that dispatches parallel evaluation to the
  registered foreach parallel backend.

  There are not options to the constructor. All configuration should be
  done through the normal interface to the foreach parallel backends.

}

\section{Methods}{

  The following generics are implemented and perform as documented on
  the corresponding help page (e.g., \code{?bpisup}):
  \code{\link{bpworkers}}, \code{\link{bpstart}}, \code{\link{bpstop}},
  \code{\link{bpisup}}, \code{\link{bpbackend}},
  \code{\link{bpbackend<-}}, \code{\link{bpvec}}.

}


\author{Martin Morgan \url{mailto:mtmorgan@fhcrc.org}}

\seealso{

  \code{getClass("BiocParallelParam")} for additional parameter classes.

  \code{register} for registering parameter classes for use in parallel
  evaluation.

  foreach for the parallel backend infrastructure used by this param
  class. TODO LINK

}

\examples{

# First register a parallel backend with foreach
library(doParallel)
registerDoParallel(cl=makeCluster(2))

p <- DoparParam()
bplapply(1:10, sqrt, BPPARAM=p)
bpvec(1:10, sqrt, BPPARAM=p)

\dontrun{
register(DoparParam(), default=TRUE)
}
}

\keyword{classes}
