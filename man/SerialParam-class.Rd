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
SerialParam(catch.errors = TRUE) 
}

\arguments{

    \item{catch.errors}{\code{logical(1)}} Flag to determine in
        apply-like functions (see e.g. \code{\link{bplapply}}) whether
        to quit with an error as soon as one application fails or
        encapsulation of function calls in \code{\link[base]{try}}
        blocks which triggers a resume mechanism (see
        \link{bpresume}). Defaults to \code{TRUE}.

}

\section{SerialParam constructor}{

  Return an object to be used for serial evaluation of otherwise
  parallel functions such as \code{\link{bplapply}},
  \code{\link{bpvec}}.
}


\section{Methods}{

  The following generics are implemented and perform as documented on
  the corresponding help page (e.g., \code{?bpworkers}):
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
p <- SerialParam()
simplify2array(bplapply(1:10, sqrt, BPPARAM=p))
bpvec(1:10, sqrt, BPPARAM=p)

\dontrun{
register(SerialParam(), default=TRUE)
}
}

\keyword{classes}
