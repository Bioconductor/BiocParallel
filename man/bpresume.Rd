\name{bpresume}

\alias{bpresume}
\alias{bplasterror}
\alias{print,remote-error-method}


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

  \code{bpresume} and \code{bplasterror} are implemented for \code{bplapply} 
  and \code{bpmapply} with \code{BatchJobsParam} only.

  \itemize{
    \item{bpresume}{

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

## These functions are applicable to bplapply and bpmapply with
## BatchJobsParam only. 

}

\keyword{manip}
\keyword{methods}
