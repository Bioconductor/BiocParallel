\name{DeveloperInterface}

\alias{.recv_all}
\alias{.recv_all,ANY-method}
\alias{.recv_any}
\alias{.recv_any,ANY-method}
\alias{.send_all}
\alias{.send_all,ANY-method}
\alias{.send_to}
\alias{.send_to,ANY-method}
\alias{.send}
\alias{.send,ANY-method}
\alias{.recv}
\alias{.recv,ANY-method}
\alias{.close}
\alias{.close,ANY-method}

\alias{.bpstart_impl}
\alias{.bpstop_impl}
\alias{.bplapply_impl}
\alias{.bpiterate_impl}

\title{Developer interface}

\description{

  Functions documented on this page are meant for developers wishing to
  implement \code{BPPARAM} objects that extend the
  \code{BiocParallelParam} virtual class to support additional parallel
  back-ends.

}

\usage{
## server interface

.send_to(cluster, node, value)
.recv_any(cluster)
.send_all(cluster, value)
.recv_all(cluster)

## client interface

.send(cluster, value)
.recv(cluster)
.close(cluster)
}

\arguments{

  \item{cluster}{
    An object containing information about the cluster, returned by
    \code{bpbackend(<BPPARAM>)}.
  }

  \item{node}{
    An integer value indicating the node in the cluster from which
    values are to be sent or received.
  }

  \item{value}{
    Any R object, to be sent to or from workers.
  }

}

\details{

  New BPPARAM implementations need to implement \code{bpstart()},
  \code{bpstop()}, \code{bplapply()}, and \code{bpiterate()}
  methods. Each method usually performs implementation-specific
  functionality before calling the next (BiocParallelParam) method.

  \itemize{

    \item \code{bpstart,BiocParallelParam-method} initiates logging, random
    number generation, and registration of finalizers to ensure that
    started clusters are stopped.

    \item \code{bpstop,BiocParallelParam-method} ensures appropriate
    clean-up of stopped clusters.

    \item \code{bplapply,ANY,BiocParallelParam-method} and
    \code{bpiterate,ANY,BiocParallelParam-method} implement: serial
    evaluation when there is a single core or task available;
    \code{BPREDO} functionality, and parallel lapply-like or iterative
    calculation.

  }

  To avoid the intricacies of multiple dispatch, the body of these
  methods is available for direct use as exported symbols
  \code{.bpstart_impl()}, \code{.bpstop_impl()},
  \code{.bplapply_impl()}, and \code{.bpiterate_impl()}.

  It is likely that new implementations will also implement
  \code{bpisup()} and \code{bpbackend()} / \code{bpbackend<-()}; there
  are no \code{BiocParallelParam} methods.

  The \emph{backends} (object returned by \code{bpbackend()}) of new
  BPPARAM implementations must support \code{length()}. In addition, the
  backends must support \code{.send_to()} and \code{.recv_any()}
  server-side and \code{.send()}, \code{.recv()}, and \code{.close()}
  client-side methods. Default \code{.send_all()} and \code{.recv_all()}
  methods are implemented as simple iterations along the
  \code{length(cluster)}, invoking \code{send_to()} or \code{recv_any()}
  on each iteration. Starting and stopping the parallel evaluation
  environment are implemented in \code{bpstart()} and \code{bpstop()}.

}

\value{

  All \code{send*} and \code{recv*} functions are endomorphic, returning a
  \code{cluster} object.

}
