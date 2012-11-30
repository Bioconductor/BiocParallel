\name{SerialParam-class}
\Rdversion{1.1}
\docType{class}
\alias{SerialParam-class}
\alias{SerialParam}

\title{Enable serial evaluation}

\description{

  This class is used to parameterize serial evaluation, primarily to
  facilitate easy transition from parallel to serial code.

}

\usage{
SerialParam(...) 
}

\arguments{

  \item{...}{Additional arguments, not yet implemented.}

}

\section{SerialParam constructor}{

  Return an object to be used for serial evaluation of otherwise
  parallel functions such as \code{\link{bplapply}},
  \code{\link{bpvec}}.

}

\section{Methods}{

  The object is \sQuote{read-only}.

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
