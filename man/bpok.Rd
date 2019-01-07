\name{bpok}

\alias{bpok}


\title{Resume computation with partial results}

\description{

  Identifies unsuccessful results returned from \code{bplapply},
  \code{bpmapply}, \code{bpvec}, \code{bpaggregate} or \code{bpvectorize}.

}

\usage{

  bpok(x)

}

\arguments{

    \item{x}{
      Results returned from a call to \code{bp*apply}.
    }

}

\details{

  \itemize{
    \item{bpok}{
      Returns a \code{logical()} vector: FALSE for any jobs that resulted in
      an error. \code{x} is the result list output by \code{bplapply},
      \code{bpmapply}, \code{bpvec}, \code{bpaggregate} or \code{bpvectorize}.
    }
  }

}

\author{Michel Lang, Martin Morgan and Valerie Obenchain}

\examples{

## -----------------------------------------------------------------------
## Catch errors: 
## -----------------------------------------------------------------------

## By default 'stop.on.error' is TRUE in BiocParallelParam objects.
SnowParam(workers = 2)

## If 'stop.on.error' is TRUE an ill-fated bplapply() simply stops,
## displaying the error message.
param <- SnowParam(workers = 2, stop.on.error = TRUE)
tryCatch({
    bplapply(list(1, "two", 3), sqrt, BPPARAM = param)
}, error=identity)

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

## A call of sqrt() on the character "2" returns an error.
param <- SnowParam(workers = 2, stop.on.error = FALSE)
X <- list(1, "two", 3)
result <- bptry(bplapply(X, sqrt, BPPARAM = param))

## Fix the input data by changing the character "2" to a numeric 2:
X_mod <- list(1, 2, 3)

## Repeat the original call to bplapply() with the partial results as 'BPREDO':
bplapply(X_mod, sqrt, BPPARAM = param , BPREDO = result)
}
