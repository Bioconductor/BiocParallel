\name{DeveloperInterface}

\alias{.BiocParallelParam_prototype}
\alias{.prototype_update}

\alias{.recv_all}
\alias{.recv_all,ANY-method}
\alias{.recv_any}
\alias{.recv_any,ANY-method}
\alias{.recv_any,SerialBackend-method}
\alias{.send_all}
\alias{.send_all,ANY-method}
\alias{.send_to}
\alias{.send_to,ANY-method}
\alias{.send_to,SerialBackend-method}
\alias{.send}
\alias{.send,ANY-method}
\alias{.recv}
\alias{.recv,ANY-method}
\alias{.close}
\alias{.close,ANY-method}


\alias{.manager}
\alias{.manager,ANY-method}
\alias{.manager_send}
\alias{.manager_send,ANY-method}
\alias{.manager_recv}
\alias{.manager_recv,ANY-method}
\alias{.manager_flush}
\alias{.manager_flush,ANY-method}
\alias{.manager_cleanup}
\alias{.manager_cleanup,ANY-method}
\alias{.manager_capacity}
\alias{.manager_capacity,ANY-method}

\alias{.bpstart_impl}
\alias{.bpstop_impl}
\alias{.bpworker_impl}
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
## class extension

.prototype_update(prototype, ...)

## manager interface

.send_to(backend, node, value)
.recv_any(backend)
.send_all(backend, value)
.recv_all(backend)

## worker interface

.send(worker, value)
.recv(worker)
.close(worker)

## optional task manager
.manager(backend)
.manager_send(manager, value)
.manager_recv(manager)
.manager_capacity(manager)
.manager_flush(manager)
.manager_cleanup(manager)

## supporting implementations

.bpstart_impl(x)
.bpworker_impl(worker)
.bplapply_impl(X, FUN, ..., BPREDO = list(), BPPARAM = bpparam())
.bpiterate_impl(ITER, FUN, ..., REDUCE, init, reduce.in.order = FALSE,
    BPPARAM = bpparam())
.bpstop_impl(x)
}

\arguments{

  \item{prototype}{
    A named \code{list} of default values for reference class
    fields.
  }

  \item{x}{
    A \code{BPPARAM} instance.
  }

  \item{backend}{
    An object containing information about the cluster, returned by
    \code{bpbackend(<BPPARAM>)}.
  }

  \item{manager}{
    An object returned by \code{.manager()}
  }

  \item{worker}{
    The object to which the worker communicates via \code{.send} and
    \code{.recv}. \code{.close} terminates the worker.
  }

  \item{node}{
    An integer value indicating the node in the backend to which
    values are to be sent or received.
  }

  \item{value}{
    Any R object, to be sent to or from workers.
  }

  \item{X, ITER, FUN, REDUCE, init, reduce.in.order, BPREDO, BPPARAM}{
    See \code{bplapply} and \code{bpiterate}.
  }

  \item{\ldots}{

    For \code{.prototype_update()}, name-value pairs to initialize
    derived and base class fields.

    For \code{.bplapply_impl()}, \code{.bpiterate_impl()}, additional
    arguments to \code{FUN()}; see \code{bplapply} and \code{bpiterate}.

  }
}

\details{

  Start a BPPARM implementation by creating a reference class, e.g.,
  extending the virtual class \code{BiocParallelParam}. Because of
  idiosyncracies in reference class field initialization, an instance of
  the class should be created by calling the generator returned by
  \code{setRefClass()} with a list of key-value pairs providing default
  parameteter arguments. The default values for the
  \code{BiocParallelParam} base class is provided in a list
  \code{.BiocParallelParam_prototype}, and the function
  \code{.prototype_update()} updates a prototype with new values,
  typically provided by the user. See the example below.

  BPPARAM implementations need to implement \code{bpstart()} and
  \code{bpstop()} methods; they may also need to implement,
  \code{bplapply()} and \code{bpiterate()} methods. Each method usually
  performs implementation-specific functionality before calling the next
  (BiocParallelParam) method.  To avoid the intricacies of multiple
  dispatch, the bodies of BiocParallelParam methods are available for
  direct use as exported symbols.

  \itemize{

    \item \code{bpstart,BiocParallelParam-method}
    (\code{.bpstart_impl()}) initiates logging, random number generation,
    and registration of finalizers to ensure that started clusters are
    stopped.

    \item \code{bpstop,BiocParallelParam-method} (\code{.bpstop_impl()})
    ensures appropriate clean-up of stopped clusters, including sending
    the DONE semaphore. \code{bpstart()} will usually arrange for
    workers to enter \code{.bpworker_impl()} to listen for and evaluate
    tasks.

    \item \code{bplapply,ANY,BiocParallelParam-method} and
    \code{bpiterate,ANY,BiocParallelParam-method}
    (\code{.bplapply_impl()}, \code{.bpiterate_impl()}) implement:
    serial evaluation when there is a single core or task available;
    \code{BPREDO} functionality, and parallel lapply-like or iterative
    calculation.

  }

  Invoke \code{.bpstart_impl()}, \code{.bpstop_impl()},
  \code{.bplapply_impl()}, and \code{.bpiterate_impl()} after any
  BPPARAM-specific implementation details.

  New implementations will also implement \code{bpisup()} and
  \code{bpbackend()} / \code{bpbackend<-()}; there are no default
  methods.

  The \emph{backends} (object returned by \code{bpbackend()}) of new
  BPPARAM implementations must support \code{length()} (number of
  nodes). In addition, the backends must support \code{.send_to()} and
  \code{.recv_any()} manager and \code{.send()}, \code{.recv()}, and
  \code{.close()} worker methods. Default \code{.send_all()} and
  \code{.recv_all()} methods are implemented as simple iterations along
  the \code{length(cluster)}, invoking \code{.send_to()} or
  \code{.recv_any()} on each iteration.

  The task manager is an optional interface for the backend who wants
  to have a control over the task dispatching process.
  \code{.manager_send()} sends the task value to a worker, \code{.manager_recv()}
  returns a list with each element being a result received from a worker.
  \code{.manager_capacity()} instructs how many
  tasks values can be processed simultaneously by the cluster. \code{.manager_flush()}
  flushes all the cached tasks(if any) immediately. \code{.manager_cleanup()} performs
  the cleanup after the job is finished. The default methods for
  \code{.manager_flush()} and \code{.manager_cleanup()} are no-op.

}

\value{

  The return value of \code{.prototype_update()} is a list with elements
  in \code{prototype} substituted with key-value pairs provided in
  \code{\ldots}.

  All \code{send*} and \code{recv*} functions are endomorphic, returning a
  \code{cluster} object.

  The return value of \code{.manager_recv()} is a list with each element being
  a result received from a worker, \code{.manager_capacity()} is
  an integer. The return values of the other \code{.manager_*()} are not restricted
}

\examples{
\donttest{
##
## Extend BiocParallelParam; `.A()` is not meant for the end user
##

.A <- setRefClass(
    "A",
    contains = "BiocParallelParam",
    fields = list(id = "character")
)

## Use a prototype for default values, including the prototype for
## inheritted fields

.A_prototype <- c(
    list(id = "default_id"),
    .BiocParallelParam_prototype
)

## Provide a constructor for the user

A <- function(...) {
    prototype <- .prototype_update(.A_prototype, ...)
    do.call(.A, prototype)
}

## Provide an R function for field access

bpid <- function(x)
    x$id

## Create and use an instance, overwriting default values

bpid(A())

a <- A(id = "my_id", threshold = "WARN")
bpid(a)
bpthreshold(a)
}
}
