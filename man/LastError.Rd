\name{LastError-class}
\Rdversion{1.0}
\docType{class}

\alias{LastError-class}
\alias{LastError}

\alias{bplapply,LastError,ANY}

\alias{show,LastError-method}

\title{Resume computation with partial results}

\description{

    This class implements a resume mechanism for partial successful execution
    for the apply-family of functions provided by BiocParallel.

}

\usage{
LastError

\S4method{show}{LastError}()
}

\arguments{}

\section{Usage}{
    The resume mechanism is triggered if the argument \code{catch.errors} of
    the \code{\link{BiocParallelParam}} class is set to \code{TRUE}.
    The methods \code{\link{bplapply}} and FIXME then store the the current
    state of computation in the \code{LastError} object.
    Recalling the method with the \code{LastError} object as first argument 
    then triggers a resume: Incomplete tasks get recomputed and merged with
    partial results computed in the previous run. \code{LastError} is updated
    if tasks throw errors in successive runs which allows for multiple iterative
    re-runs.
}

\section{Slots and Methods}{
    You can retrieve a list of partial results using the slot
    \code{results}. Errornous parts are stored as a \code{\link[base]{simpleError}}
    object. The slot \code{is.error} contains a logical vector with \code{TRUE}
    for parts of \code{results} which terminated with an exception.

    The \code{show} method provides a summarizing overview over the stored
    state of computation.
}

\author{Martin Morgan \url{mailto:mtmorgan@fhcrc.org}}


\examples{
\dontrun{
p <- SerialParam(catch.errors = TRUE)
f <- function(x) if (x %% 2 == 0) x else stop(x)
print(LastError)
bplapply(1:4, f, BPPARAM=p)
print(LastError)
bplapply(LastError, identity, BPPARAM=p)
}
}
