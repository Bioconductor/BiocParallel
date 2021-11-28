\name{bpok}

\alias{bpok}
\alias{bperrorTypes}
\alias{bpresult}


\title{Resume computation with partial results}

\description{

  Identifies unsuccessful results returned from \code{bplapply},
  \code{bpmapply}, \code{bpvec}, \code{bpaggregate} or \code{bpvectorize}.

}

\usage{

  bpok(x, type = bperrorTypes())

  bperrorTypes()

  bpresult(x)

}

\arguments{

    \item{x}{
      Results returned from a call to \code{bp*apply}.
    }

    \item{type}{
      A character(1) error type, from the vector returned by
      \code{bperrorTypes()} and described below
    }

}

\details{

  \code{bpok()} returns a \code{logical()} vector: FALSE for any jobs
  that resulted in an error. \code{x} is the result list output by
  \code{bplapply}, \code{bpmapply}, \code{bpvec}, \code{bpaggregate} or
  \code{bpvectorize}.

  \code{bperrorTypes()} returns a character() vector of possible error
  types generated during parallel evaluation. Types are:

  \itemize{
    \item{\code{bperror}: Any of the following errors. This is the
      default value for \code{bpok()}.}

    \item{\code{remote_error}: An \emph{R} error occurring while
      evaluating \code{FUN()}, e.g., taking the square root of a
      character vector, \code{sqrt("One")}.}

    \item{\code{unevaluated_error}: When \code{*Param(stop.on.error =
        TRUE)} (default), a remote error halts evaluation of other
        tasks assigned to the same worker. The return value for these
        unevaluated elements is an error of type
        \code{unevaluated_error}.}

    \item{\code{not_available_error}: Only produced by
      \code{DoparParam()} when a remote error occurs during evaluation
      of an element of \code{X} -- \code{DoparParam()} sets all values
      after the remote error to this class.}

    \item{\code{worker_comm_error}: An error occurring while trying to
      communicate with workers, e.g., when a worker quits unexpectedly.
      when this type of error occurs, the length of the result may
      differ from the length of the input \code{X}.
    }

  }

  \code{bpresult()} when applied to an object with a class of one of the
  error types returns the list of tasks results.

}

\author{Michel Lang, Martin Morgan, Valerie Obenchain, and Jiefei Wang}

\seealso{
  \code{\link{tryCatch}}
}

\examples{

## -----------------------------------------------------------------------
## Catch errors: 
## -----------------------------------------------------------------------

## By default 'stop.on.error' is TRUE in BiocParallelParam objects.  If
## 'stop.on.error' is TRUE an ill-fated bplapply() simply stops,
## displaying the error message.
param <- SnowParam(workers = 2, stop.on.error = TRUE)
result <- tryCatch({
    bplapply(list(1, "two", 3), sqrt, BPPARAM = param)
}, error=identity)
result
class(result)
bpresult(result)

## If 'stop.on.error' is FALSE then the computation continues. Errors
## are signalled but the full evaluation can be retrieved
param <- SnowParam(workers = 2, stop.on.error = FALSE)
X <- list(1, "two", 3)
result <- bptry(bplapply(X, sqrt, BPPARAM = param))
result

## Check for errors:
fail <- !bpok(result)
fail

## Access the traceback with attr():
tail(attr(result[[2]], "traceback"), 5)

## -----------------------------------------------------------------------
## Resume calculations: 
## -----------------------------------------------------------------------

## The 'resume' mechanism is triggered by supplying a list of partial
## results as 'BPREDO'. Data elements that failed are rerun and merged
## with previous results.

## A call of sqrt() on the character "2" returns an error. Fix the input
## data by changing the character "2" to a numeric 2:
X_mod <- list(1, 2, 3)
bplapply(X_mod, sqrt, BPPARAM = param , BPREDO = result)
}
