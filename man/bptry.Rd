\name{bptry}

\alias{bptry}

\title{Try expression evaluation, recovering from bperror signals}

\description{

  This function is meant to be used as a wrapper around
  \code{bplapply()} and friends, returning the evaluated expression
  rather than signalling an error.

}
\usage{
bptry(expr, bplist_error, ...)
}

\arguments{
  \item{expr}{An R expression; see \code{\link{tryCatch}}.}

  \item{bplist_error}{

    A \sQuote{handler} function of a single argument, used to catch
    \code{bplist_error} conditions signalled by
    \code{expr}. \code{bplist_error} conditions are signalled when an
    element of \code{bplapply} and other iterations contain a evaluation
    that failed. When missing, the default retrieves the \dQuote{result}
    attribute from the error, containing the partially evaluated
    results.

  }

  \item{\dots}{Additional named handlers passed to \code{tryCatch()}.}

}

\value{

  The partially evaluated list of results.

}

\author{Martin Morgan \email{martin.morgan@roswellpark.org}}


\seealso{
  \code{\link{tryCatch}}, \code{\link{bplapply}}.
}
\examples{
param = registered()[[1]]
X = list(1, "2", 3)
tryCatch(bplapply(X, sqrt), error=identity)  # error summarizing results
bptry(bplapply(X, sqrt))                     # list including error
}

\keyword{manip}
