\name{MulticoreParam-class}
\Rdversion{1.1}
\docType{class}

\alias{MulticoreParam}
\alias{MulticoreParam-class}

\alias{multicoreWorkers}
\alias{bpisup,MulticoreParam-method}
\alias{bpschedule,MulticoreParam-method}

\alias{bpvec,ANY,MulticoreParam-method}
\alias{show,MulticoreParam-method}

\title{Enable multi-core parallel evaluation}

\description{
  This class is used to parameterize single computer multicore parallel
  evaluation on non-Windows computers. \code{multicoreWorkers()} chooses
  the number of workers based on operating system (Windows only supports
  1 core), global user preference (\code{options(mc.cores=...)}), or the
  minimum of 8 and the number of detected cores (\code{detectCores()}).
}

\usage{
MulticoreParam(workers = multicoreWorkers(), 
               catch.errors = TRUE, stop.on.error = FALSE,
               log = FALSE, threshold = "INFO", logdir = character(),
               resultdir = character(), setSeed = TRUE,
               recursive = TRUE, cleanup = TRUE, cleanupSignal = tools::SIGTERM,
               verbose = FALSE, ...)

multicoreWorkers()
}

\details{
  \code{MulticoreParam} is used for shared memory computing. Under the hood
  the cluster is created with \code{SnowParam(..., type="FORK")}. See
  ?\code{SnowParam} for a description of error handling, logging
  and writing out results.
}

\arguments{
  \item{workers}{
    \code{integer(1)} Number of workers. Defaults to all cores available as
    determined by \code{detectCores}.
  }
  \item{catch.errors}{
    \code{logical(1)} Enable the catching of errors and warnings.
  }
  \item{stop.on.error}{
    \code{logical(1)} Enable stop on error.
  }
  \item{log}{
    \code{logical(1)} Enable logging.
  }
  \item{threshold}{
    \code{character(1)} Logging threshold as defined in \code{futile.logger}.
  }
  \item{logdir}{
    \code{character(1)} Log files directory. When not provided, log
    messages are returned to stdout.
  }
  \item{resultdir}{
    \code{character(1)} Job results directory. When not provided, results
    are returned as an \R{} object (list) to the workspace.
  }
  \item{setSeed}{
    \code{logical(1)} as described in parallel::mcparallel argument 
    \code{mc.set.seed}.
  }
  \item{recursive}{
    \code{logical(1)} indicating whether recursive calls are evaluated in
    parallel; see parallel::mclapply argument \code{mc.allow.recursive}.
  }
  \item{cleanup}{
    \code{logical(1)} indicating whether forked children will be
    terminated before \code{bplapply} returns, as for parallel::mclapply
    argument \code{cleanup}. If TRUE, then the signal sent to the child
    is \code{cleanupSignal}.
  }
  \item{cleanupSignal}{
    \code{integer(1)} the signal sent to forked
    processes when \code{cleanup=TRUE}.
  }
  \item{verbose}{
    \code{logical(1)} when \code{TRUE} echo stdout of
    forked processes. This is the complement of
    \code{parallel::mclapply}'s argument \code{mc.silent}.
  }
  \item{\dots}{
    Additional arguments passed to \code{\link[snow]{makeCluster}}
  }
}

\section{Constructor}{
  \describe{
    \item{}{
      \code{MulticoreParam(workers = multicoreWorkers(), 
                           catch.errors = TRUE, stop.on.error = FALSE,
                           log = FALSE, threshold = "INFO",
                           logdir = character(), resultdir = character(), 
                           cleanupSignal = tools::SIGTERM, 
                           verbose = FALSE, ...)}:

        Return an object representing a FORK cluster. The cluster is not
        created until \code{bpstart} is called. Named arguments in \code{...} 
        are passed to \code{makeCluster}.
    }
  }
}

\section{Accessors}{
  In the code below, \code{x} is a \code{MulticoreParam} object. See the 
  ?\code{bpcontols} man page for full details on these accessors.

  \describe{
    \item{}{\code{bpworkers(x, ...)}}
    \item{}{\code{bpstart(x, ...)}}
    \item{}{\code{bpstop(x, ...)}}
    \item{}{\code{bpisup(x, ...)}}
    \item{}{\code{bpbackend(x, ...)}, \code{bpbackend(x) <- value}: 
            \code{value} must be on of \code{SOCK} or \code{MPI}.}
  }
  See the ?\code{BiocParallelParam} man page for details on these accessors.
  \describe{
    \item{}{\code{bpcatchErrors(x, ...)}, \code{bpcatchErrors(x) <- value}}
    \item{}{\code{bpstopOnError(x, ...)}, \code{bpstopOnError(x) <- value}}
    \item{}{\code{bplog(x, ...)}, \code{bplog(x) <- value}}
    \item{}{\code{bpthreshold(x, ...)}, \code{bpthreshold(x) <- value}} 
    \item{}{\code{bplogdir(x, ...)}, \code{bplogdir(x) <- value}} 
    \item{}{\code{bpresultdir(x, ...)}, \code{bpresultdir(x) <- value}} 
  }
}

\section{Other Methods}{
  \describe{
    \item{}{
      \code{show(x)}:
      Displays the \code{MulticoreParam} object. 
    }
    \item{}{
      \code{bpok(x)}:
      Returns a \code{logical()} vector: FALSE for any jobs that resulted in
      an error. \code{x} is the result list output by a \code{BiocParallel}
      function such as \code{bplapply} or \code{bpmapply}. 
    }
  }
}

\author{Martin Morgan \url{mailto:mtmorgan@fhcrc.org} and Valerie Obenchain}

\seealso{
  \itemize{
    \item \code{register} for registering parameter classes for use in 
          parallel evaluation.

    \item \code{\link{SnowParam}} for computing in distributed memory

    \item \code{\link{BatchJobsParam}} for computing with cluster schedulers

    \item \code{\link{SerialParam}} for non-parallel evaluation 
  }
}

\examples{
multicoreWorkers()
p <- MulticoreParam()
bplapply(1:10, sqrt, BPPARAM=p)
bpvec(1:10, sqrt, BPPARAM=p)

\dontrun{
register(MulticoreParam(), default=TRUE)
}

bpworkers(SerialParam())

\dontrun{
  p <- SnowParam(2L)
  bpworkers(p)          # 2 local nodes, communicating via sockets
  bpstart(p)            # start cluster
  bplapply(1:10, sqrt, BPPARAM=p)
  bpstop(p)             # stop cluster

  p <- SnowParam(4L)
  bplapply(1:10, sqrt, BPPARAM=p)  # automatically start / stop cluster
}  

}

\keyword{classes}
\keyword{methods}
