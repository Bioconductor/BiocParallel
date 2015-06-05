\name{bpok}

\alias{bpresume}
\alias{bplasterror}
\alias{bpok}


\title{Resume computation with partial results}

\description{

  Identifies unsuccessful results returned from \code{bplapply},
  \code{bpmapply}, \code{bpvec}, \code{bpaggregate} or \code{bpvectorize}.

  \code{bpresume} and \code{bplaterror} have been deprecated.

}

\usage{

  bpok(x)

  ## Deprected:
  bpresume(expr)
  bplasterror()

}

\arguments{

    \item{x}{
      Results returned from a call to \code{bp*lapply}.
    }
    \item{expr}{
      A expression to be re-evaluated. If the original error was due to 
      input error, \code{X} should be modified. If hardware limitations
      or failure caused the error this expression may be the same as the
      original.
    }

}

\details{

  \itemize{
    \item{bpok}{
      Returns a \code{logical()} vector: FALSE for any jobs that resulted in
      an error. \code{x} is the result list output by \code{bplapply},
      \code{bpmapply}, \code{bpvec}, \code{bpaggregate} or \code{bpvectorize}.
    }
    \item{bpresume}{
      THIS FUNCTION IS DEPRECATED.
      The resume mechanism allows computations with errors to be re-attempted
      and is triggered when the argument \code{catch.errors} is \code{TRUE}.
      
      Unsuccessful results returned from \code{bp*lapply} can be identified
      with \code{bpok}. Failure may have been due to faulty input or hardware
      error. Incomplete portions of the job can be reattempted  with
      \code{bpresume}. New results are merged with the previous and returned to
      the user.
    }
    \item{bplasterror}{
      THIS FUNCTION IS DEPRECATED.
      Use \code{attr} on the output of \code{bp*apply}
      to see traceback. See examples.
    }
  }

}

\author{Michel Lang, Martin Morgan and Valerie Obenchain}

\examples{

## -----------------------------------------------------------------------
## Catch errors: 
## -----------------------------------------------------------------------

## By default 'catch.errors' is TRUE in BiocParallelParam objects.
SnowParam(workers = 2)

## If 'catch.errors' is FALSE an ill-fated bplapply() simply stops
## displaying the error message.
param <- SnowParam(workers = 2, catch.errors = FALSE)
\dontrun{
> res <- bplapply(list(1, "two", 3), sqrt, BPPARAM = param)
Error in checkForRemoteErrors(val) : 
  one node produced an error: non-numeric argument to mathematical function
}

## When 'catch.errors' is TRUE partial results are returned with 
## the error.
param <- SnowParam(workers = 2)
X <- list(1, "two", 3)
res <- bplapply(X, sqrt, BPPARAM = param)
res

## Check if the results contain any errors:
fail <- !bpok(res)
fail

## Access the traceback with attr():
tail(attr(res[[2]], "traceback"), 5)

## -----------------------------------------------------------------------
## Resume calculations: 
## -----------------------------------------------------------------------

## The 'resume' mechanism is triggered by supplying a list of partial
## results as 'BPREDO'. Data elements that failed are rerun and merged
## with previous results.

## A call of sqrt() on the character "2" returns an error.
param <- SnowParam(workers = 2)
X <- list(1, "two", 3)
res <- bplapply(X, sqrt, BPPARAM = param)
res

## Fix the input data by changing the character "2" to a numeric 2:
X_mod <- list(1, 2, 3)

## Repeat the original call to bplapply() with the partial results as 'BPREDO':
bplapply(X_mod, sqrt, BPPARAM = param , BPREDO = res)
}





