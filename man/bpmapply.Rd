\name{bpmapply}
\alias{bpmapply}
\alias{bpmapply,ANY,list-method}
\alias{bpmapply,ANY,missing-method}
\alias{bpmapply,ANY,BiocParallelParam-method}

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
    BPREDO=list(), BPPARAM=bpparam(), BPOPTIONS = bpoptions())

\S4method{bpmapply}{ANY,missing}(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE,
    USE.NAMES=TRUE, BPREDO=list(), BPPARAM=bpparam(), BPOPTIONS = bpoptions())

\S4method{bpmapply}{ANY,BiocParallelParam}(FUN, ..., MoreArgs=NULL,
    SIMPLIFY=TRUE, USE.NAMES=TRUE, BPREDO=list(),
    BPPARAM=bpparam(), BPOPTIONS = bpoptions())

}

\arguments{

  \item{FUN}{The \code{function} to be applied to each element passed
    via \code{...}.
  }
  \item{\dots}{Objects for which methods \code{length}, \code{[}, and
    \code{[[} are implemented. All objects must have the same length or
    shorter objects will be replicated to have length equal to the
    longest.
  }
  \item{MoreArgs}{List of additional arguments to \code{FUN}.
  }
  \item{SIMPLIFY}{
    If \code{TRUE} the result will be simplified using
    \code{\link{simplify2array}}.
  }
  \item{USE.NAMES}{If \code{TRUE} the result will be named.
  }
  \item{BPPARAM}{An optional \code{\link{BiocParallelParam}} instance
    defining the parallel back-end to be used during evaluation.
  }
  \item{BPREDO}{A \code{list} of output from \code{bpmapply} with one or
    more failed elements. When a list is given in \code{BPREDO},
    \code{bpok} is used to identify errors, tasks are rerun and inserted
    into the original results.
  }
  \item{BPOPTIONS}{
    Additional options to control the behavior of the parallel evaluation, see \code{\link{bpoptions}}.
  }

}

\details{

  See \code{methods{bpmapply}} for additional methods, e.g.,
  \code{method?bpmapply("MulticoreParam")}.

}

\value{See \code{\link[base]{mapply}}.}

\author{

  Michel Lang . Original code as attributed in
  \code{\link{mclapply}}.

}

\seealso{

  \itemize{
    \item \code{\link{bpvec}} for parallel, vectorized calculations.

    \item \code{\link{BiocParallelParam}} for possible values of \code{BPPARAM}.
  }
}

\examples{
methods("bpmapply")

fun <- function(greet, who) {
    paste(Sys.getpid(), greet, who)
}
greet <- c("morning", "night")
who <- c("sun", "moon")

param <- bpparam()
original <- bpworkers(param)
bpworkers(param) <- 2
result <- bpmapply(fun, greet, who, BPPARAM = param)
cat(paste(result, collapse="\n"), "\n")
bpworkers(param) <- original
}

\keyword{manip}






