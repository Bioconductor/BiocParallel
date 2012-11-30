\name{SnowParam-class}
\Rdversion{1.1}
\docType{class}

\alias{SnowParam}
\alias{SnowParam-class}

\alias{bplapply,ANY,ANY,SnowParam-method}
\alias{bpvec,ANY,ANY,SnowParam-method}
\alias{bparallelize,ANY,SnowParam-method}

\alias{show,SnowParam-method}

\title{Enable simple network of workstations (SNOW) parallelism}

\description{

  This class is used to parameterize simple network of workstations
  (SNOW) parallelism on one or several physical computers.

}

\usage{
SnowParam(spec, type, ...) 

\S4method{bplapply}{ANY,ANY,SnowParam}(X, FUN, ..., param)

\S4method{bpvec}{ANY,ANY,SnowParam}(X, FUN, ..., param)

\S4method{bparallelize}{ANY,SnowParam}(X, ..., param)
}

\arguments{

  \item{X}{A vector-like object supporting \code{length}, \code{[},
    and, for \code{bplapply}, \code{[[}.}

  \item{FUN}{A function operating on \code{X} and other arguments in
    \code{...}.}

  \item{...}{Additional arguments to \code{FUN}, or for classes derived
    from \code{MulticoreParam}.}

  \item{param}{An \code{MulticoreParam}-class instance.}

  \item{spec}{A cluster specification, as in
    \code{\link[parallel]{makeCluster}} argument \code{spec}.}

  \item{type}{\code{character(1)} type of cluster to use, as described
    in \code{\link[parallel]{parLapply}} argument \code{type}.}

}

\section{SnowParam constructor}{

  Return an object resulting from invoking \code{makeCluster} with
  arguments \code{spec} and \code{type}. The cluster is created when the
  object is instantiated. It does not make sense to save the object
  across sessions, but it may be used multiple times in a session.

}

\author{Martin Morgan \url{mailto:mtmorgan@fhcrc.org}}

\seealso{

  \code{getClass("BiocParallelParam")} for additional parameter classes.

  \code{register} for registering parameter classes for use in parallel
  evaluation.

}

\examples{
param <- SnowParam(2L)
bplapply(1:10, sqrt, param=param)
bpvec(1:10, sqrt, param=param)

\dontrun{
register(SnowParam(2L), default=TRUE)
}
}

\keyword{classes}
