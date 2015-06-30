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


\alias{show,DoparParam-method}

\title{Enable parallel evaluation using registered dopar backend}

\description{

  This class is used to dispatch parallel operations to the dopar
  backend registered with the foreach package.

}

\usage{
DoparParam(catch.errors = TRUE)
}

\details{

  \code{DoparParam} can be used for shared or non-shared memory computing
  depending on what backend is loaded. The \code{doSNOW} package supports 
  non-shared memory, \code{doParallel} supports both shared and non-shared. 
  When not specified, the default number of workers in \code{DoparParam}
  is determined by \code{getDoParWorkers()}. See the \code{foreach} package 
  vignette for details using the different backends:

  \url{http://cran.r-project.org/web/packages/foreach/vignettes/foreach.pdf}

}

\arguments{

    \item{catch.errors}{\code{logical(1)}} Flag to determine in
        apply-like functions (see e.g. \code{\link{bplapply}}) whether
        to quit with an error as soon as one application fails or
        encapsulation of function calls in \code{\link[base]{try}}
        blocks which triggers a resume mechanism (see
        \link{bpresume}). Defaults to \code{TRUE}.  }

\section{DoparParam constructor}{

  Return a proxy object that dispatches parallel evaluation to the
  registered foreach parallel backend.

  There are no options to the constructor. All configuration should be
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

  \code{foreach-package} for the parallel backend infrastructure
  used by this param class.

}

\examples{

# First register a parallel backend with foreach
library(doParallel)
cl <- makeCluster(2)
registerDoParallel(cl)

p <- DoparParam()
bplapply(1:10, sqrt, BPPARAM=p)
bpvec(1:10, sqrt, BPPARAM=p)

stopCluster(cl)
\dontrun{
register(DoparParam(), default=TRUE)
}
}

\keyword{classes}
