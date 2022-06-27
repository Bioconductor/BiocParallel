\name{SerialParam-class}
\Rdversion{1.1}
\docType{class}
\alias{SerialParam-class}
\alias{SerialParam}

\alias{bpbackend,SerialParam-method}
\alias{bpstart,SerialParam-method}
\alias{bpstop,SerialParam-method}
\alias{bpisup,SerialParam-method}
\alias{bpworkers,SerialParam-method}
\alias{bplog,SerialParam-method}
\alias{bplogdir,SerialParam-method}
\alias{bplog<-,SerialParam,logical-method}
\alias{bpthreshold<-,SerialParam,character-method}
\alias{bplogdir<-,SerialParam,character-method}
\alias{length,SerialBackend-method}

\title{Enable serial evaluation}

\description{

  This class is used to parameterize serial evaluation, primarily to
  facilitate easy transition from parallel to serial code.

}

\usage{
SerialParam(
    stop.on.error = TRUE,
    progressbar = FALSE,
    RNGseed = NULL,
    timeout = WORKER_TIMEOUT,
    log = FALSE,
    threshold = "INFO",
    logdir = NA_character_,
    resultdir = NA_character_,
    jobname = "BPJOB",
    force.GC = FALSE
)
}
\details{

  \code{SerialParam} is used for serial computation on a single
  node. Using \code{SerialParam} in conjunction with \code{bplapply}
  differs from use of \code{lapply} because it provides features such as
  error handling, logging, and random number use consistent with
  \code{SnowParam} and \code{MulticoreParam}.

  \describe{
    \item{error handling:}{

      By default all computations are attempted and partial results
      are returned with any error messages.
      \itemize{

        \item \code{stop.on.error} A \code{logical}. Stops all jobs as soon
              as one job fails or wait for all jobs to terminate. When
              \code{FALSE}, the return value is a list of successful results
              along with error messages as 'conditions'.

        \item The \code{bpok(x)} function returns a \code{logical()} vector
              that is FALSE for any jobs that threw an error. The input
              \code{x} is a list output from a bp*apply function such as
              \code{bplapply} or \code{bpmapply}.
      }
    }
    \item{logging:}{
      When \code{log = TRUE} the \code{futile.logger} package is loaded on
      the workers. All log messages written in the \code{futile.logger} format
      are captured by the logging mechanism and returned real-time
      (i.e., as each task completes) instead of after all jobs have finished.

      Messages sent to \emph{stdout} and \emph{stderr} are returned to
      the workspace by default. When \code{log = TRUE} these
      are diverted to the log output. Those familiar with the \code{outfile}
      argument to \code{makeCluster} can think of \code{log = FALSE} as
      equivalent to \code{outfile = NULL}; providing a \code{logdir} is the
      same as providing a name for \code{outfile} except that BiocParallel
      writes a log file for each task.

      The log output includes additional statistics such as memory use
      and task runtime. Memory use is computed by calling gc(reset=TRUE)
      before code evaluation and gc() (no reseet) after. The output of the
      second gc() call is sent to the log file. There are many ways to
      track memory use - this particular approach was taken because it is
      consistent with how the BatchJobs package reports memory on the
      workers.
    }
    \item{log and result files:}{
      Results and logs can be written to a file instead of returned to
      the workspace. Writing to files is done from the master as each task
      completes. Options can be set with the \code{logdir} and
      \code{resultdir} fields in the constructor or with the accessors,
      \code{bplogdir} and \code{bpresultdir}.
    }
    \item{random number generation:}{
      For \code{MulticoreParam}, \code{SnowParam}, and
      \code{SerialParam}, random number generation is controlled through
      the \code{RNGseed = } argument. BiocParallel uses the
      L'Ecuyer-CMRG random number generator described in the parallel
      package to generate independent random number streams. One stream
      is associated with each element of \code{X}, and used to seed the
      random number stream for the application of \code{FUN()} to
      \code{X[[i]]}. Thus setting \code{RNGseed = } ensures
      reproducibility across \code{MulticoreParam()},
      \code{SnowParam()}, and \code{SerialParam()}, regardless of worker
      or task number. The default value \code{RNGseed = NULL} means that
      each evaluation of \code{bplapply} proceeds independently.

      For details of the L'Ecuyer generator, see ?\code{clusterSetRNGStream}.
    }
  }

}

\section{Constructor}{

  \describe{
    \item{}{
      \code{SerialParam()}:

      Return an object to be used for serial evaluation of otherwise
      parallel functions such as \code{\link{bplapply}},
      \code{\link{bpvec}}.
    }
  }

}

\arguments{

  \item{stop.on.error}{
    \code{logical(1)} Enable stop on error.
  }
  \item{progressbar}{
    \code{logical(1)} Enable progress bar (based on plyr:::progress_text).
  }
  \item{RNGseed}{
    \code{integer(1)} Seed for random number generation. The seed is
    used to set a new, independent random number stream for each
    element of \code{X}. The ith element recieves the same stream seed,
    regardless of use of \code{SerialParam()}, \code{SnowParam()}, or
    \code{MulticoreParam()}, and regardless of worker or task
    number. When \code{RNGseed = NULL}, a random seed is used.
  }
  \item{timeout}{
    \code{numeric(1)} Time (in seconds) allowed for worker to complete a task.
    This value is passed to base::setTimeLimit() as both the \code{cpu} and
    \code{elapsed} arguments. If the computation exceeds \code{timeout} an
    error is thrown with message 'reached elapsed time limit'.
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
  \item{jobname}{
    \code{character(1)} Job name that is prepended to log and result files.
    Default is "BPJOB".
  }
  \item{force.GC}{
    \code{logical(1)} Whether to invoke the garbage collector after each
    call to \code{FUN}. The default (\code{FALSE}, do not explicitly
    call the garbage collection) rarely needs to be changed.
  }

}

\section{Methods}{

  The following generics are implemented and perform as documented on
  the corresponding help page (e.g., \code{?bpworkers}):
  \code{\link{bpworkers}}. \code{\link{bpisup}}, \code{\link{bpstart}},
  \code{\link{bpstop}}, are implemented, but do not have any
  side-effects.

}

\author{Martin Morgan \url{mailto:mtmorgan@fhcrc.org}}

\seealso{

  \code{getClass("BiocParallelParam")} for additional parameter classes.

  \code{register} for registering parameter classes for use in parallel
  evaluation.

}

\examples{
p <- SerialParam()
simplify2array(bplapply(1:10, sqrt, BPPARAM=p))
bpvec(1:10, sqrt, BPPARAM=p)
}

\keyword{classes}
