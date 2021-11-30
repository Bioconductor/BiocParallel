\name{bptry}

\alias{bptry}

\title{Try expression evaluation, recovering from bperror signals}

\description{

  This function is meant to be used as a wrapper around
  \code{bplapply()} and friends, returning the evaluated expression
  rather than signalling an error.

}
\usage{
bptry(expr, ..., bplist_error, bperror)
}

\arguments{
  \item{expr}{An R expression; see \code{\link{tryCatch}}.}

  \item{bplist_error}{

    A \sQuote{handler} function of a single argument, used to catch
    \code{bplist_error} conditions signalled by \code{expr}. A
    \code{bplist_error} condition is signalled when an element of
    \code{bplapply} and other iterations contain a evaluation that
    failed. When missing, the default retrieves the \dQuote{result}
    attribute from the error, containing the partially evaluated
    results.

    Setting \code{bplist_error=identity} returns the evaluated
    condition.

    Setting \code{bplist_error=stop} passes the condition to other
    handlers, notably the handler provided by \code{bperror}.

  }

  \item{bperror}{

    A \sQuote{handler} function of a single argument, use to catch
    \code{bperror} conditions signalled by \code{expr}. A \code{bperror}
    is a base class to all errors signaled by \pkg{BiocParallel}
    code. When missing, the default returns the condition without
    signalling an error.

  }

  \item{\dots}{

    Additional named handlers passed to \code{tryCatch()}. These
    user-provided handlers are evaluated before default handlers
    \code{bplist_error}, \code{bperror}.

  }

}

\value{

  The partially evaluated list of results.

}

\author{Martin Morgan \email{martin.morgan@roswellpark.org}}


\seealso{
  \code{\link{bpok}}, \code{\link{tryCatch}}, \code{\link{bplapply}}.
}
\examples{
param = registered()[[1]]
param
X = list(1, "2", 3)
bptry(bplapply(X, sqrt))                         # bplist_error handler
result <- bptry(bplapply(X, sqrt), bplist_error=identity)  # bperror handler
result
bpresult(result)
}

\keyword{manip}
