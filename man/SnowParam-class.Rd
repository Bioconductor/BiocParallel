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


\alias{show,SnowParam-method}

\title{Enable simple network of workstations (SNOW) parallel evaluation}

\description{

  This class is used to parameterize simple network of workstations
  (SNOW) parallel evaluation on one or several physical computers.

}

\usage{
SnowParam(workers = 0L, type, catch.errors = TRUE, ...)

## invoke as(cl, "SnowParam")
\S4method{coerce}{SOCKcluster,SnowParam}(from, to)
}

\arguments{

    \item{workers}{\code{integer(1)}} Number of workers on with
        Multicore and SSH backend which defaults here to all workers
        available.  On managed HPC \code{workers} defaults to \code{NA}
        but can be set to control chunking of jobs.  See argument
        \code{n.chunks} in \code{\link[BBmisc]{chunk}} and
        \code{\link[BatchJobs]{submitJobs}} for more information.

    \item{type}{\code{character(1)} type of cluster to use, as described
        in \code{\link[parallel]{clusterMap}} argument \code{type}.}

    \item{catch.errors}{\code{logical(1)} Flag to determine in
        apply-like functions (see e.g. \code{\link{bplapply}}) whether
        to quit with an error as soon as one application fails or
        encapsulation of function calls in \code{\link[base]{try}}
        blocks which triggers a resume mechanism (see
        \link{bpresume}). Defaults to \code{TRUE}.}

    \item{\dots}{Additional arguments passed to
        \code{\link[parallel]{makeCluster}}}

    \item{from, to}{(N.B. Use \code{as(from, "SnowParam")} to coerce
        from a cluster created with, e.g.,
        \code{parallel::makeCluster}). \code{from} is a
        \code{SOCKcluster} or derived instance (e.g., from
        \code{parallel::makeCluster}), to be coerced to a
        \code{SnowParam} instance.}  }

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
