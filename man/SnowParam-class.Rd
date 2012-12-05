\name{SnowParam-class}
\Rdversion{1.1}
\docType{class}

\alias{SnowParam}
\alias{SnowParam-class}

\alias{bpbackend,SnowParam-method}
\alias{bpbackend<-,SnowParam,SOCKcluster-method}

\alias{bpisup,SnowParam-method}
\alias{bpstart,SnowParam-method}
\alias{bpstop,SnowParam-method}
\alias{bpworkers,SnowParam-method}

\alias{bplapply,ANY,ANY,SnowParam-method}

\alias{show,SnowParam-method}

\title{Enable simple network of workstations (SNOW) parallelism}

\description{

  This class is used to parameterize simple network of workstations
  (SNOW) parallelism on one or several physical computers.

}

\usage{
SnowParam(workers = 0L, type, ...) 

\S4method{bplapply}{ANY,ANY,SnowParam}(X, FUN, ..., param)

}

\arguments{

  \item{X}{A vector-like object supporting \code{length}, \code{[},
    and, for \code{bplapply}, \code{[[}.}

  \item{FUN}{A function operating on \code{X} and other arguments in
    \code{...}.}

  \item{...}{Additional arguments to \code{FUN}, or for classes derived
    from \code{MulticoreParam}.}

  \item{param}{An \code{MulticoreParam}-class instance.}

  \item{workers}{Number of workers to start the cluster, as in
    \code{\link[parallel]{makeCluster}} argument \code{spec}.}

  \item{type}{\code{character(1)} type of cluster to use, as described
    in \code{\link[parallel]{parLapply}} argument \code{type}.}

}

\section{SnowParam constructor}{

  Return an object representing a SNOW cluster. The cluster is not
  created until \code{bpstart} is called.

  \code{bpstart} creates the cluster by invoking \code{makeCluster} with
  arguments \code{spec=workers}, \code{type}, and other arguments passed
  to \code{...} in \code{SnowParam}.

}

\section{Methods}{

  The following generics are implemented and perform as documented on
  the corresonding help page (e.g., \code{?bpisup}):
  \code{\link{bpworkers}}, \code{\link{bpstart}}, \code{\link{bpstop}},
  \code{\link{bpisup}}, \code{\link{bpbackend}},
  \code{\link{bpbackend<-}}, \code{\link{bpvec}}.

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
