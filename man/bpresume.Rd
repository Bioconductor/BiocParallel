\name{bpresume}
\alias{bpresume}
\alias{bplasterror}


\title{Resume computation with partial results}

\description{
   Resume partial successful calls to  \code{\link{bplapply}} or 
   \code{\link{bpmapply}}
}

\usage{
    bplasterror()
    bpresume(expr)
}

\arguments{
    \item{expr}{expression}
       A expression which calls either \code{bplapply} or \code{bpmapply}.
}

\details{

  \itemize{
    \item{bpresume}{
      Implemented for \code{bplapply} and \code{bpmapply} only. Supported
      for \code{BatchJobsParam} and \code{DoparParam} only.

      The resume mechanism is triggered if the argument \code{catch.errors} of
      the \code{\link{BiocParallelParam}} class is set to \code{TRUE}.  The
      \code{\link{bplapply}} and \code{\link{bpmapply}} methods store the
      current state of computation marking which jobs were successful and which
      returned an error.

      There are two approaches to re-computing jobs that returned errors. The
      first is to set \code{BPRESUME=TRUE} in the call to \code{bplapply} or
      \code{bpmapply}. Alternatively, if the call to \code{\link{bplapply}} and
      \code{\link{bpmapply}} is inside a function and not directly accessible to
      the user, the last call can be embedded in \code{bpresume()}. Wrapping in
      \code{bpresume} sets an option accordingly to enable the resume feature
      down in the call stack.  In both cases, only the computations with errors
      are re-attempted.  New results are merged with the previous and returned
      to the user.
    }
    \item{bplasterror}{
      Returns a \code{LastError} object containing the partial results and 
      errors to investigate.
    }
  }

    Note that nested calls of the apply functions can cause troubles in
    some scenarios.

}

\author{Martin Morgan \url{mailto:mtmorgan@fhcrc.org}}

\examples{

## -----------------------------------------------------------------------
## Catch errors: 
## -----------------------------------------------------------------------

## By default 'catch.errors' is TRUE in BiocParallelParam objects.
SnowParam(workers = 2)

## If 'catch.errors' is FALSE an ill-fated bplapply() simply stops
## displaying the error message.
snowp <- SnowParam(workers = 2, catch.errors = FALSE)
\dontrun{
> res <- bplapply(list(1, "two", 3), sqrt, BPPARAM = snowp)
Error in checkForRemoteErrors(val) : 
  one node produced an error: non-numeric argument to mathematical function
}

## When 'catch.errors' is TRUE the same call provides traceback
## information (truncated here) and suggests use of bplasterror() and
## bpresume().
snowp <- SnowParam(workers = 2)
\dontrun{
> res <- bplapply(list(1, "two", 3), sqrt, BPPARAM = snowp)
Error: 1 errors; first error:
  Error in FUN(...): non-numeric argument to mathematical function

For more information, use bplasterror(). To resume calculation, re-call
  the function and set the argument 'BPRESUME' to TRUE or wrap the
  previous call in bpresume().

First traceback:
  19: parallel:::.slaveRSOCK()
  18: slaveLoop(makeSOCKmaster(master, port, timeout, useXDR))
...
}

## bplasterror() reports the number of successful results and error message.
\dontrun{
> bplasterror()
2 / 3 partial results stored. First 1 error messages:
[2]: Error in FUN(...): non-numeric argument to mathematical function
}

## -----------------------------------------------------------------------
## Resume calculations: 
## -----------------------------------------------------------------------

## The 'resume' mechanism attempts to re-run the data element that failed.
## In our example the character "two" list element will never succeed. In 
## the runs below we replace the character with a numeric and compute the
## result.

## There are two approaches for resuming a suspended calculation, one 
## is to wrap the original call in bpresume().
\dontrun{
> bpresume(res <- bplapply(list(1, 2, 3), sqrt, BPPARAM = snowp))
Resuming previous calculation... 
> res
[[1]]
[1] 1

[[2]]
[1] 1.414214

[[3]]
[1] 1.732051
}

## Another approach is to set 'BPRESUME = TRUE' in bplapply().
\dontrun{
res <- bplapply(list(1, 2, 3), sqrt, BPPARAM = snowp, BPRESUME = TRUE)
}
}
