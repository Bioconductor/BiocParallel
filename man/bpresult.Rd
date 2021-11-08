\name{bpresult}

\alias{bpresult}

\title{Access task results}

\description{

  This function is used to access the task results and errors from
  \code{bplapply()} and \code{bpiterate()}, including when an error is
  captured by \code{bptry()} or \code{tryCatch()} and returned to the
  user.

}

\usage{
bpresult(x)
}

\arguments{

  \item{x}{An object returned by \code{bplapply()} or
    \code{bpiterate()}, perhaps using \code{bptry()}, or
    \code{tryCatch()}.}

}

\value{

  If \code{x} is of class \code{bplist_error}, a list of task
  results. Otherwise, the object \code{x} itself as a list with class
  \code{bpresult}.

}

\author{Jiefei Wang}

\seealso{

  \code{\link{tryCatch}}, \code{\link{bptry}}, \code{\link{bplapply}},
  \code{\link{bpiterate}}.

}
\examples{

param <- SerialParam()
FUN <- function(i) {
    if (i == 2)
        stop("unexpected value: ", i)
    i
}
X <- 1:3
result1 <- tryCatch({
    bplapply(X, FUN, BPPARAM = param)
}, error = identity)
result1
bpresult(result1)  # access the task results

## bpresult(result1) is the same as...
result2 <- bptry(bplapply(X, FUN, BPPARAM = param))
result2

iterator <- function(x) {
    force(x)
    i <- 0L
    function() {
        i <<- i + 1L
        if (i <= length(x)) x[i]
    }
}
result3 <- tryCatch({
   bpiterate(iterator(X), FUN, BPPARAM = param)
}, error = identity)
result3
bpresult(result3)

bptry(bpiterate(iterator(X), FUN, BPPARAM = param))

}

\keyword{manip}
