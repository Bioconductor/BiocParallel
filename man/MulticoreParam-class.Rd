\name{MulticoreParam-class}
\Rdversion{1.1}
\docType{class}

\alias{MulticoreParam}
\alias{MulticoreParam-class}

\alias{bpisup,MulticoreParam-method}
\alias{bpschedule,MulticoreParam-method}

\alias{bpvec,ANY,MulticoreParam-method}
\alias{bparallelize,ANY,MulticoreParam-method}

\alias{show,MulticoreParam-method}

\title{Enable multi-core parallel evaluation}

\description{

  This class is used to parameterize single computer multicore parallel
  evaluation on non-Windows computers.

}

\usage{
MulticoreParam(workers = detectCores(), catch.errors = TRUE,
    setSeed = TRUE, recursive = TRUE, cleanup = TRUE,
    cleanupSignal = tools::SIGTERM, verbose = FALSE, ...) 
}

\arguments{

    \item{workers}{\code{integer(1)}} Number of workers on with
        Multicore and SSH backend which defaults here to all workers
        available.  On managed HPC \code{workers} defaults to \code{NA}
        but can be set to control chunking of jobs.  See argument
        \code{n.chunks} in \code{\link[BBmisc]{chunk}} and
        \code{\link[BatchJobs]{submitJobs}} for more information.

    \item{catch.errors}{\code{logical(1)}} Flag to determine in
        apply-like functions (see e.g. \code{\link{bplapply}}) whether
        to quit with an error as soon as one application fails or
        encapsulation of function calls in \code{\link[base]{try}}
        blocks which triggers a resume mechanism (see
        \link{bpresume}). Defaults to \code{TRUE}.

    \item{setSeed}{\code{logical(1)}, as described in
        \code{parallel::mcparallel} argument \code{mc.set.seed}.}

    \item{recursive}{\code{logical(1)} indicating whether recursive calls
        are evaluated in parallel; see \code{parallel::mclapply}
        argument \code{mc.allow.recursive}.}

    \item{cleanup}{\code{logical(1)} indicating whether forked children
        will be terminated before \code{bplapply} returns, as for
        \code{parallel::mclapply} argument \code{cleanup}. If
        \code{TRUE}, then the signal sent to the child is
        \code{cleanupSignal}.}

    \item{cleanupSignal}{\code{integer(1)} the signal sent to forked
        processes when \code{cleanup=TRUE}.}

    \item{verbose}{\code{logical(1)} when \code{TRUE} echo stdout of
        forked processes. This is the complement of
        \code{parallel::mclapply}'s argument \code{mc.silent}.}

    \item{\dots}{Additional arguments passed to
        \code{\link[parallel]{mclapply}}}

}

\section{MulticoreParam constructor}{

  Return an object with specified values. The object may be saved to
  disk or reused within a session.

}

\section{Methods}{

  The following generics are implemented and perform as documented on
  the corresponding help page (e.g., \code{?bpisup}):
  \code{\link{bpworkers}}, \code{\link{bpstart}}, \code{\link{bpstop}},
  \code{\link{bpisup}}, \code{\link{bpschedule}},
  \code{\link{bpbackend}}.

}

\author{Martin Morgan \url{mailto:mtmorgan@fhcrc.org}}

\seealso{

  \code{getClass("BiocParallelParam")} for additional parameter classes.

  \code{register} for registering parameter classes for use in parallel
  evaluation.

}

\examples{
p <- MulticoreParam()
bplapply(1:10, sqrt, BPPARAM=p)
bpvec(1:10, sqrt, BPPARAM=p)

\dontrun{
register(MulticoreParam(), default=TRUE)
}
}

\keyword{classes}
