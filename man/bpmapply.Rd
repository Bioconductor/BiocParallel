\name{bpmapply}
\alias{bpmapply}
\alias{bpmapply,ANY,ANY-method}
\alias{bpmapply,ANY,missing-method}
\alias{bpmapply,ANY,BiocParallelParam-method}
\alias{bpmapply,ANY,BatchJobsParam-method}
\alias{bpmapply,ANY,DoparParam-method}
\alias{bpmapply,ANY,MulticoreParam-method}
\alias{bpmapply,ANY,SerialParam-method}
\alias{bpmapply,ANY,SnowParam-method}

\title{Parallel mapply-like functionality}

\description{

  \code{bpmapply} applies \code{FUN} to first elements of \code{...},
  the second elements and so on.  Any type of object in \code{...} is
  allowed, provided \code{length}, \code{[}, and \code{[[} methods are
  available. The return value is a \code{list} of length equal to the
  length of all objects provided, as with \code{\link[base]{mapply}}.

}

\usage{

bpmapply(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
    BPPARAM=bpparam())

\S4method{bpmapply}{ANY,ANY}(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, 
    USE.NAMES=TRUE, BPPARAM=bpparam())

\S4method{bpmapply}{ANY,missing}(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, 
    USE.NAMES=TRUE, BPPARAM=bpparam())

\S4method{bpmapply}{ANY,BiocParallelParam}(FUN, ..., MoreArgs=NULL, 
    SIMPLIFY=TRUE, USE.NAMES=TRUE, BPPARAM=bpparam())

}

\arguments{

  \item{FUN}{The \code{function} to be applied to each element passed
    via \code{...}.}

  \item{\dots}{Objects for which methods \code{length}, \code{[}, and
    \code{[[} are implemented. All objects must have the same length or
    shorter objects will be replicated to have length equal to the
    longest.}

  \item{MoreArgs}{List of additional arguments to \code{FUN}.}

  \item{SIMPLIFY}{
    If \code{TRUE} the result will be simplified using
    \code{\link[base]{simplify2array}}.}

  \item{USE.NAMES}{If \code{TRUE} the result will be named.}


  \item{BPPARAM}{An optional \code{\link{BiocParallelParam}} instance
    defining the parallel back-end to be used during evaluation.}

}

\details{
  
  See \code{showMethods{bpmapply}} for additional methods, e.g.,
  \code{method?bpmapply("MulticoreParam")}.

}

\value{See \code{\link[base]{mapply}}.}

\author{
  
  Michel Lang . Original code as attributed in
  \code{\link[parallel]{mclapply}}.

}

\seealso{

  \itemize{
    \item \code{\link{bpvec}} for parallel, vectorized calculations.

    \item \code{\link{BiocParallelParam}} for possible values of \code{BPPARAM}.
  }
}

\examples{
showMethods("bpmapply")

## ten tasks (1:10) so ten calls to FUN default registered parallel
## back-end. Compare with bpvec.
result <- bpmapply(function(greet, who) {
    paste(Sys.getpid(), greet, who)
}, c("morning", "night"), c("sun", "moon"))
cat(paste(result, collapse="\n"), "\n")
}

\keyword{manip}
