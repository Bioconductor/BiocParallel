\name{SnowParam-class}
\Rdversion{1.1}
\docType{class}

\alias{SnowParam-class}
\alias{SnowParam}
\alias{coerce,SOCKcluster,SnowParam-method}

\alias{bpbackend,SnowParam-method}
\alias{bpbackend<-,SnowParam,SOCKcluster-method}

\alias{bpisup,SnowParam-method}
\alias{bpstart,SnowParam-method}
\alias{bpstop,SnowParam-method}
\alias{bpworkers,SnowParam-method}

\alias{bplapply,ANY,SnowParam-method}

\alias{show,SnowParam-method}

\title{Enable simple network of workstations (SNOW) parallel evaluation}

\description{

  This class is used to parameterize simple network of workstations
  (SNOW) parallel evaluation on one or several physical computers.

}

\usage{
SnowParam(workers = 0L, type, ...) 

## invoke as(cl, "SnowParam")
\S4method{coerce}{SOCKcluster,SnowParam}(from, to)

\S4method{bplapply}{ANY,SnowParam}(X, FUN, ..., BPPARAM)
}

\arguments{

  \item{workers}{Number of workers to start the cluster, as in
    \code{\link[parallel]{makeCluster}} argument \code{spec}.}

  \item{type}{\code{character(1)} type of cluster to use, as described
    in \code{\link[parallel]{parLapply}} argument \code{type}.}

  \item{from, to}{(N.B. Use \code{as(from, "SnowParam")} to coerce from
    a cluster created with, e.g.,
    \code{parallel::makeCluster}). \code{from} is a \code{SOCKcluster}
    or derived instance (e.g., from \code{parallel::makeCluster}), to be
    coerced to a \code{SnowParam} instance.}

  \item{X}{A vector-like object supporting \code{length}, \code{[},
    and, for \code{bplapply}, \code{[[}.}

  \item{FUN}{A function operating on \code{X} and other arguments in
    \code{...}.}

  \item{...}{Additional arguments to \code{FUN}, or for classes derived
    from \code{MulticoreParam}.}

  \item{BPPARAM}{An \code{MulticoreParam}-class instance.}

}

\section{SnowParam constructor}{

  Return an object representing a SNOW cluster. The cluster is not
  created until \code{bpstart} is called.

  \code{bpstart} creates the cluster by invoking \code{makeCluster} with
  arguments \code{spec=workers}, \code{type}, and other arguments passed
  to \code{...} in \code{SnowParam}.

  Use \code{as(cl, "SnowParam")} to coerce a cluster created directly by
  \code{parallel::param} to a \code{SnowParam} instance. Instances
  created in this way cannot be started or stopped.

}

\section{Methods}{

  The following generics are implemented and perform as documented on
  the corresponding help page (e.g., \code{?bpisup}):
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
p <- SnowParam(2L)
bplapply(1:10, sqrt, BPPARAM=p)
bpvec(1:10, sqrt, BPPARAM=p)

\dontrun{
register(SnowParam(2L), default=TRUE)
}
}

\keyword{classes}
