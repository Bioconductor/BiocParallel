\name{bpresume}
\alias{bpresume}
\alias{bplasterror}


\title{Resume computation with partial results}

\description{
   Resume partial successful calls to  \code{\link{bplapply}} or \code{\link{bpmapply}}
}

\usage{
    bplasterror()
    bpresume(expr)
}

\arguments{
    \item{expr}{expression}
        A expression which calls either \code{bplapply} or \code{bpmapply}.
}

\section{Usage}{

    The resume mechanism is triggered if the argument
    \code{catch.errors} of the \code{\link{BiocParallelParam}} class is
    set to \code{TRUE}.  The methods \code{\link{bplapply}} and
    \code{\link{bpmapply}} then store the current state of computation.
    Recalling the call directory with argument \code{BPRESUME} set to
    \code{TRUE} will then only compute the missing parts of the previous
    call and merge the results.

    Alternatively, if the call to \code{\link{bplapply}} and
    \code{\link{bpmapply}} is inside a function not accessible directly
    by the user, the last call can be embedded into \code{bpresume}
    which sets an option accordingly to enable the resume feature down
    in the call stack.

    The function \code{bplasterror} returns a \code{LastError} object
    containing the partial results and errors to investigate.

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

## If 'catch.errors' is FALSE an ill-fated bplapply simply stops
## displaying the error message.
snowp <- SnowParam(workers = 2, catch.errors = FALSE)
\dontrun{
> res <- bplapply(list(1, "two", 3), sqrt, BPPARAM=snowp)
Error in checkForRemoteErrors(val) : 
  one node produced an error: non-numeric argument to mathematical function
}

## When 'catch.errors' is TRUE an ill-fated bplapply provides traceback
## information (truncated here) and suggests use of bplasterror() and
## bpresume().
snowp <- SnowParam(workers = 2)
\dontrun{
> res <- bplapply(list(1, "two", 3), sqrt, BPPARAM=snowp)
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

## The 'resume' mechanism tries to re-run the data element that failed.
## In our example the "two" list element will never succeed so it must be
## modified. 

## There are two methods for resuming a suspended calculation:
## One approach is to wrap the original call in bpresume().
\dontrun{
> bpresume(res <- bplapply(list(1, 2, 3), sqrt, BPPARAM=snowp))
Resuming previous calculation... 
> res
[[1]]
[1] 1

[[2]]
[1] 1.414214

[[3]]
[1] 1.732051
}

## The second is to set 'BPRESUME = TRUE' in the call to bplapply().
\dontrun{
res <- bplapply(list(1, 2, 3), sqrt, BPPARAM=snowp, BPRESUME=TRUE)
}
}
