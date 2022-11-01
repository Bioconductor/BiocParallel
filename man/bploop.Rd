\name{bploop}
\Rdversion{1.1}

% Class
\alias{bploop}

% managers
\alias{bploop.lapply}
\alias{bploop.iterate}

\alias{bprunMPIworker}

\title{Internal Functions for SNOW-style Parallel Evaluation}

\description{

  The functions documented on this page are primarily for use within
  \pkg{BiocParallel} to enable SNOW-style parallel evaluation, using
  communication between manager and worker nodes through sockets.

}

\usage{
\S3method{bploop}{lapply}(manager, X, FUN, ARGS, BPPARAM, BPOPTIONS = bpoptions(), BPREDO, ...)

\S3method{bploop}{iterate}(manager, ITER, FUN, ARGS, BPPARAM, BPOPTIONS = bpoptions(),
       REDUCE, BPREDO, init, reduce.in.order, ...)
}

\arguments{

  \item{manager}{An object representing the manager node. For workers,
    this is the node to which the worker will communicate. For managers,
    this is the form of iteration -- \code{lapply} or \code{iterate}.}

  \item{X}{A vector of jobs to be performed.}

  \item{FUN}{A function to apply to each job.}

  \item{ARGS}{A list of arguments to be passed to \code{FUN}.}

  \item{BPPARAM}{An instance of a \code{BiocParallelParam} class.}

  \item{ITER}{A function used to generate jobs. No more jobs are
    available when \code{ITER()} returns \code{NULL}.}

  \item{REDUCE}{(Optional) A function combining two values returned by
    \code{FUN} into a single value.}

  \item{init}{(Optional) Initial value for reduction.}

  \item{reduce.in.order}{(Optional) logical(1) indicating that
    reduction must occur in the order jobs are dispatched
    (\code{TRUE}) or that reduction can occur in the order jobs are
    completed (\code{FALSE}).}

  \item{BPREDO}{(Optional) A \code{list} of output from \code{bplapply}
   or \code{bpiterate} with one or more failed elements.}

  \item{\ldots}{Additional arguments, ignored in all cases.}

  \item{BPOPTIONS}{
    Additional options to control the behavior of the parallel evaluation, see \code{\link{bpoptions}}.
  }
}

\details{

  Workers enter a loop. They wait to receive a message (\R list) from
  the \code{manager}. The message contains a \code{type} element, with
  evaluation as follows:

  \describe{

    \item{\dQuote{EXEC}}{Execute the \R{} code in the message, returning
      the result to the \code{manager}.}

    \item{\dQuote{DONE}}{Signal termination to the \code{manager},
      terminate the worker.}

  }

  Managers under \code{lapply} dispatch pre-determined jobs, \code{X},
  to workers, collecting the results from and dispatching new jobs to
  the first available worker.  The manager returns a list of results, in
  a one-to-one correspondence with the order of jobs supplied, when all
  jobs have been evaluated.

  Managers under \code{iterate} dispatch an undetermined number of jobs
  to workers, collecting previous jobs from and dispatching new jobs to
  the first available worker. Dispatch continues until available jobs
  are exhausted. The return value is by default a list of results in a
  one-to-one correspondence with the order of jobs supplied. The return
  value is influenced by \code{REDUCE}, \code{init}, and
  \code{reduce.in.order}.

}

\author{

  Valerie Obenchain, Martin Morgan. Derived from similar functionality
  in the \pkg{snow} and \pkg{parallel} packages.

}

\examples{
## These functions are not meant to be called by the end user.
}
