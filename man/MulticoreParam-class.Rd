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

## constructor 
## ------------------------------------

MulticoreParam(workers = multicoreWorkers(), tasks = 0L, 
               catch.errors = TRUE, stop.on.error = FALSE,
               log = FALSE, threshold = "INFO", 
               logdir = character(), resultdir = character(), ...)

## detect workers 
## ------------------------------------

multicoreWorkers()

}

\details{

  \code{MulticoreParam} is used for shared memory computing. Under the hood
  the cluster is created with \code{SnowParam(..., type="FORK")}. See
  ?\code{SnowParam} for a description of error handling, logging
  and writing out results.

}

\section{Constructor}{

  \describe{
    \item{}{
      \code{MulticoreParam(workers = multicoreWorkers(), tasks = 0L, 
                           catch.errors = TRUE, stop.on.error = FALSE,
                           tasks = 0L, log = FALSE, threshold = "INFO",
                           logdir = character(), resultdir = character(), 
                           ...)}:

        Return an object representing a FORK cluster. The cluster is not
        created until \code{bpstart} is called. Named arguments in \code{...} 
        are passed to \code{makeCluster}.
    }
  }

}

\arguments{

  \item{workers}{
    \code{integer(1)} Number of workers. Defaults to all cores available as
    determined by \code{detectCores}.
  }
  \item{tasks}{
    \code{integer(1)}. The number of tasks per job. \code{value} must be a 
    scalar integer >= 0L.
 
    In this documentation a job is defined as a single call to a function,
    \code{bplapply}, \code{bpmapply} etc. A task is the division of the \code{X}
    argument into chunks. When \code{tasks == 0} (default), \code{X} is divided
    by the number of workers. This approach distributes \code{X} in
    (approximately) equal chunks.
 
    A \code{tasks} value of > 0 dictates the total number of tasks. Values
    can range from 1 (all of \code{X} to a single worker) to the length of 
    \code{X} (each element of \code{X} to a different worker). 
 
    When the length of \code{X} is less than the number of workers each
    element of \code{X} is sent to a worker and \code{tasks} is ignored.
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
  \item{\dots}{
    Additional arguments passed to \code{\link[snow]{makeCluster}}
  }

}

\section{Accessors: Logging and results}{

  In the following code, \code{x} is a \code{MulticoreParam} object.
  \describe{
    \item{}{
      \code{bplog(x, ...)}, \code{bplog(x) <- value}: 
      Get or set the value to enable logging. \code{value} must be a
      \code{logical(1)}.
    }
    \item{}{
      \code{bpthreshold(x, ...)}, \code{bpthreshold(x) <- value}: 
      Get or set the logging threshold. \code{value} must be a
      \code{character(1)} string of one of the levels defined in the
      \code{futile.logger} package: \dQuote{TRACE}, \dQuote{DEBUG},
      \dQuote{INFO}, \dQuote{WARN}, \dQuote{ERROR}, or \dQuote{FATAL}.
    }
    \item{}{
      \code{bplogdir(x, ...)}, \code{bplogdir(x) <- value}: 
      Get or set the directory for the log file. \code{value} must be a
      \code{character(1)} path, not a file name. The file is written out as
      LOGFILE.out. If no \code{logdir} is provided and \code{bplog=TRUE} log
      messages are sent to stdout.
    }
    \item{}{
      \code{bpresultdir(x, ...)}, \code{bpresultdir(x) <- value}: 
      Get or set the directory for the result files. \code{value} must be a
      \code{character(1)} path, not a file name. Separate files are written for
      each job with the prefix JOB (e.g., JOB1, JOB2, etc.).  When no
      \code{resultdir} is provided the results are returned to the session as
      \code{list}.
    }
  }

}

\section{Accessors: Back-end control}{

  In the code below \code{x} is a \code{MulticoreParam} object.  See the
  ?\code{BiocParallelParam} man page for details on these accessors.
  \describe{
    \item{}{\code{bpworkers(x, ...)}}
    \item{}{\code{bptasks(x, ...)}, \code{bptasks(x) <- value}}
    \item{}{\code{bpstart(x, ...)}}
    \item{}{\code{bpstop(x, ...)}}
    \item{}{\code{bpisup(x, ...)}}
    \item{}{\code{bpbackend(x, ...)}, \code{bpbackend(x) <- value}}
  }

} 

\section{Accessors: Error Handling}{

  In the code below \code{x} is a \code{MulticoreParam} object. See the
  ?\code{BiocParallelParam} man page for details on these accessors.
  \describe{
    \item{}{\code{bpcatchErrors(x, ...)}, \code{bpcatchErrors(x) <- value}}
    \item{}{\code{bpstopOnError(x, ...)}, \code{bpstopOnError(x) <- value}}
  }

}

\section{Methods: Evaluation}{

  In the code below \code{BPPARAM} is a \code{MulticoreParam} object.
  Full documentation for these functions are on separate man pages: see
  ?\code{bpmapply}, ?\code{bplapply}, ?\code{bpvec}, ?\code{bpiterate} and
  ?\code{bpaggregate}.
  \describe{
    \item{}{ 
      \code{bpmapply(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, 
                     USE.NAMES=TRUE, BPPARAM=bpparam())}
    }
    \item{}{
      \code{bplapply(X, FUN, ..., BPPARAM=bpparam())}
    }
    \item{}{
      \code{bpvec(X, FUN, ..., AGGREGATE=c, BPPARAM=bpparam())}
    }
    \item{}{
      \code{bpiterate(ITER, FUN, ..., BPPARAM=bpparam())}
    }
    \item{}{
      \code{bpaggregate(x, data, FUN, ..., BPPARAM=bpparam())}
    }
  }

}

\section{Methods: Other}{

  In the code below \code{x} is a \code{MulticoreParam} object.
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

    \item \code{\link{DoparParam}} for computing with foreach 

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
}

\keyword{classes}
\keyword{methods}
