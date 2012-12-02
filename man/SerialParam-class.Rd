\name{SerialParam-class}
\Rdversion{1.1}
\docType{class}
\alias{SerialParam-class}
\alias{SerialParam}

\alias{bpisup,SerialParam-method}
\alias{bpworkers,SerialParam-method}

\title{Enable serial evaluation}

\description{

  This class is used to parameterize serial evaluation, primarily to
  facilitate easy transition from parallel to serial code.

}

\usage{
SerialParam() 
}

\section{SerialParam constructor}{

  Return an object to be used for serial evaluation of otherwise
  parallel functions such as \code{\link{bplapply}},
  \code{\link{bpvec}}.

  The object is \sQuote{read-only}.

}


\section{Methods}{

  The following generics are implemented and perform as documented on
  the corresonding help page (e.g., \code{?bpworkers}):
  \code{\link{bpworkers}}. \code{\link{bpisup}}, \code{\link{bpstart}},
  \code{\link{bpstop}}, are implemented, but do not have any
  side-effects.

}

\author{Martin Morgan \url{mailto:mtmorgan@fhcrc.org}}

\seealso{

  \code{getClass("BiocParallelParam")} for additional parameter classes.

  \code{register} for registering parameter classes for use in parallel
  evaluation.

}

\examples{
param <- SerialParam()
simplify2array(bplapply(1:10, sqrt, param=param))
bpvec(1:10, sqrt, param=param)

\dontrun{
register(SerialParam(), default=TRUE)
}
}

\keyword{classes}
