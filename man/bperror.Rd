\name{bperror}

\alias{bperror}

\title{Accessing task results}

\description{

  This function is used to access the task results and errors from
  \code{bplapply()} and friends when an error is captured by \code{trycatch()}
  and returned to the user.
}

\usage{
bperror(x)
}

\arguments{
  \item{x}{ A \code{bplist_error} object}
}

\value{

  If \code{x} is of the class \code{bplist_error}, a list of task results. Otherwise, the object \code{x} itself.

}

\author{Martin Morgan \email{martin.morgan@roswellpark.org}}


\seealso{
  \code{\link{tryCatch}}, \code{\link{bptry}}, \code{\link{bplapply}}.
}
\examples{
param = registered()[[1]]
param
X = list(1, "2", 3)
res = tryCatch(bplapply(X, sqrt), error = identity)  # catch the error
bperror(res)  # access the task results
}

\keyword{manip}
