\title{Parallel evaluation on a single computer}
\name{LocalParam-class}
\alias{}
\alias{.close,local_worker-method}
\alias{.recv,local_worker-method}
\alias{.recv_any,local_cluster-method}
\alias{.send,local_worker-method}
\alias{.send_to,local_cluster-method}
\alias{bpbackend,LocalParam-method}
\alias{bpisup,LocalParam-method}
\alias{bpschedule,LocalParam-method}
\alias{bpstart,LocalParam-method}
\alias{bpstop,LocalParam-method}

\description{

  \code{LocalParam()} is used for parallel evaluation on a single
  physical computer. Unlike \code{MulticoreParam()}, \code{LocalParam()}
  does not require access to particular ports. Also, \code{LocalParam()}
  is implemented across platforms. On Windows, \code{LocalParam()} uses
  independent processes for parallelization, while on unix (Linux and
  macOS) \code{LocalParam()} uses forked processes.

}
\usage{

LocalParam(workers = snowWorkers(), ..., cluster = c("auto", "snow"))

}
\arguments{
  \item{workers}{
    \code{integer(1)} number of workers. Defaults to all cores available as
    determined by \code{detectCores}.}

  \item{...}{
    Additional paramters influencing job size, error handling, logging,
    etc. See \code{\link{BiocParallelParam}}.}

  \item{cluster}{
    \code{character(1)} type of cluster to create on the local
    computer. \code{"auto"} creates forked processes on Linux and macOS,
    and independent processes on Windows. \code{"snow"} creates
    independent processes on Linux, macOS, and Windows.}
}
\details{

  \code{LocalParam()} uses 'local' unix-style sockets for communication
  between manager and worker. The primary advantage to this approach is
  that local sockets do not require open ports for
  communication. Performance may also be improved slightly because
  complex protocols are not required to resolve destinations.

  \code{LocalParam()} differs from \code{SnowParam()} or
  \code{MulticoreParam()} in that it supports both forked and independet
  processes on Linux and macOS (only independent processes are supported
  on Windows). The default is to use forked processes when available.

}
\value{
  A \code{LocalParam()} instance.
}
\examples{
p <- LocalParam(4, cluster = "snow")
table(unlist(bplapply(1:1000, function(i) Sys.getpid(), BPPARAM=p)
}
