\name{MulticoreParam-class}
\Rdversion{1.1}
\docType{class}

\alias{MulticoreParam}
\alias{MulticoreParam-class}

\alias{bpisup,MulticoreParam-method}

\alias{bplapply,ANY,ANY,MulticoreParam-method}
\alias{bpvec,ANY,ANY,MulticoreParam-method}
\alias{bparallelize,ANY,MulticoreParam-method}

\alias{show,MulticoreParam-method}

\title{Enable multi-core parallelism}

\description{

  This class is used to parameterize single computer multicore parallel
  evaluation on non-Windows computers.

}

\usage{
MulticoreParam(workers = detectCores(), setSeed = TRUE, recursive = TRUE, 
    cleanup = TRUE, cleanupSignal = tools::SIGTERM, verbose = FALSE, 
    ...) 

\S4method{bplapply}{ANY,ANY,MulticoreParam}(X, FUN, ..., param)

\S4method{bpvec}{ANY,ANY,MulticoreParam}(X, FUN, ..., param)

\S4method{bparallelize}{ANY,MulticoreParam}(X, ..., param)
}

\arguments{

  \item{X}{A vector-like object supporting \code{length}, \code{[},
    and, for \code{bplapply}, \code{[[}.}

  \item{FUN}{A function operating on \code{X} and other arguments in
    \code{...}.}

  \item{...}{Additional arguments to \code{FUN}, or for classes derived
    from \code{MulticoreParam}.}

  \item{param}{An \code{MulticoreParam}-class instance.}

  \item{workers}{\code{integer(1)} number of cores to use for parallel
    evaluation.}

  \item{setSeed}{\code{logical(1)}, as described in
    \code{\link[parallel]{mcparallel}} argument \code{mc.set.seed}.}

  \item{recursive}{\code{logical(1)} indicating whether recursive calls
    are parallelized; see \code{\link[parallel]{mclapply}} argument
    \code{mc.allow.recursive}.}

  \item{cleanup}{\code{logical(1)} indicating whether forked children
    will be terminated before \code{bplapply} returns, as for
    \code{\link[parallel]{mclapply}} argument \code{cleanup}. If
    \code{TRUE}, then the signal sent to the child is
    \code{cleanupSignal}.}

  \item{cleanupSignal}{\code{integer(1)} the signal sent to forked
    processes when \code{cleanup=TRUE}.}

  \item{verbose}{\code{logical(1)} when \code{TRUE} echo stdout of
    forked processes. This is the complement of
    \code{\link[parallel]{mclapply}}'s argument \code{mc.silent}.}

}

\section{MulticoreParam constructor}{

  Return an object with specified values. The object may be saved to
  disk or reused within a session.

}

\section{Methods}{

  The following generics are implemented and perform as documented on
  the corresonding help page (e.g., \code{?bpisup}):
  \code{\link{bpworkers}}, \code{\link{bpstart}}, \code{\link{bpstop}},
  \code{\link{bpisup}}, \code{\link{bpbackend}}.

}

\author{Martin Morgan \url{mailto:mtmorgan@fhcrc.org}}

\seealso{

  \code{getClass("BiocParallelParam")} for additional parameter classes.

  \code{register} for registering parameter classes for use in parallel
  evaluation.

}

\examples{
param <- MulticoreParam()
bplapply(1:10, sqrt, param=param)
bpvec(1:10, sqrt, param=param)

\dontrun{
register(MulticoreParam(), default=TRUE)
}
}

\keyword{classes}
