\name{MulticoreParam-class}
\Rdversion{1.1}
\docType{class}

\alias{MulticoreParam}
\alias{MulticoreParam-class}

\alias{multicoreWorkers}
\alias{bpisup,MulticoreParam-method}
\alias{bpschedule,MulticoreParam-method}
\alias{bpworkers<-,MulticoreParam,numeric-method}
\alias{show,MulticoreParam-method}

%% implementation detail
\alias{.close,TransientMulticoreParam-method}
\alias{.recv,TransientMulticoreParam-method}
\alias{.recv_all,TransientMulticoreParam-method}
\alias{.recv_any,TransientMulticoreParam-method}
\alias{.send,TransientMulticoreParam-method}
\alias{.send_to,TransientMulticoreParam-method}
\alias{bpbackend,TransientMulticoreParam-method}
\alias{bpstart,TransientMulticoreParam-method}
\alias{bpstop,TransientMulticoreParam-method}
\alias{length,TransientMulticoreParam-method}

\title{Enable multi-core parallel evaluation}

\description{

  This class is used to parameterize single computer multicore parallel
  evaluation on non-Windows computers. \code{multicoreWorkers()} chooses
  the number of workers.

}

\usage{

## constructor
## ------------------------------------

MulticoreParam(workers = multicoreWorkers(), tasks = 0L,
               stop.on.error = TRUE,
               progressbar = FALSE, RNGseed = NULL,
               timeout = WORKER_TIMEOUT, exportglobals=TRUE,
               log = FALSE, threshold = "INFO", logdir = NA_character_,
               resultdir = NA_character_, jobname = "BPJOB",
               force.GC = TRUE, fallback = TRUE,
               manager.hostname = NA_character_, manager.port = NA_integer_,
               ...)

## detect workers
## ------------------------------------

multicoreWorkers()

}

\details{

  \code{MulticoreParam} is used for shared memory computing. Under the hood
  the cluster is created with \code{makeCluster(..., type ="FORK")} from
  the \code{parallel} package.

  The default number of workers is determined by
  \code{multicoreWorkers()}. On windows, the number of multicore workers
  is always 1. Otherwise, the default is determined by an environment
  variable \code{R_PARALLELLY_AVAILABLECORES_FALLBACK} (see the
  parallely package) or \code{BIOCPARALLEL_WORKER_NUMBER} or, if those
  are not set, the maximum of 1 and \code{parallel::detectCores() -
  2}. Machines with 3 or fewer cores, or machines where number of cores
  cannot be determined, are assigned a single worker. Machines with more
  than 127 cores are limited to the number of \emph{R} connections
  available when the workers start; this is 128 (a hard-coded limit in
  \emph{R}) minus the number of open connections as returned by
  \code{nrow(showConnections(all=TRUE))}. The option \code{mc.cores} can
  be used to specify an arbitrary number of workers, e.g.,
  \code{options(mc.cores=4L)}. The \emph{Bioconductor} build system
  enforces a maximum of 4 workers. The environment variable
  \code{_R_CHECK_LIMIT_CORES_} (as on CRAN) enforces a maximum of 2
  workers.

  A FORK transport starts workers with the \code{mcfork} function and
  communicates between master and workers using socket connections.
  \code{mcfork} builds on fork() and thus a Linux cluster is not supported.
  Because FORK clusters are Posix based they are not supported on
  Windows. When \code{MulticoreParam} is created/used in Windows it
  defaults to \code{SerialParam} which is the equivalent of using a
  single worker.

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
      are captured by the logging mechanism and returned in real-time
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
      \code{MulticoreParam(workers = multicoreWorkers(), tasks = 0L,
                           stop.on.error = FALSE,
                           tasks = 0L, progressbar = FALSE, RNGseed = NULL,
                           timeout = Inf, exportglobals=TRUE,
                           log = FALSE, threshold = "INFO",
                           logdir = NA_character_, resultdir = NA_character_,
                           manager.hostname = NA_character_,
                           manager.port = NA_integer_, ...)}:

        Return an object representing a FORK cluster. The cluster is not
        created until \code{bpstart} is called. Named arguments in \code{...}
        are passed to \code{makeCluster}.
    }
  }

}

\arguments{

  \item{workers}{
    \code{integer(1)} Number of workers. Defaults to the maximum of 1 or
    the number of cores determined by \code{detectCores} minus 2 unless
    environment variables \code{R_PARALLELLY_AVAILABLECORES_FALLBACK} or
    \code{BIOCPARALLEL_WORKER_NUMBER} are set otherwise.
  }
  \item{tasks}{
    \code{integer(1)}. The number of tasks per job. \code{value} must be a
    scalar integer >= 0L.

    In this documentation a job is defined as a single call to a function, such
    as \code{bplapply}, \code{bpmapply} etc. A task is the division of the
    \code{X} argument into chunks.

    When \code{tasks == 0} (default, except when \code{progressbar =
    TRUE}), \code{X} is divided as evenly as possible over the number of
    workers.

    A \code{tasks} value of > 0 specifies the exact number of
    tasks. Values can range from 1 (all of \code{X} to a single worker)
    to the length of \code{X} (each element of \code{X} to a different
    worker).

    When the length of \code{X} is less than the number of workers each
    element of \code{X} is sent to a worker and \code{tasks} is ignored.

    When the length of \code{X} is less than \code{tasks}, \code{tasks}
    is treated as \code{length(X)}.
  }
    \item{stop.on.error}{
    \code{logical(1)} Enable stop on error.
  }
  \item{progressbar}{
    \code{logical(1)} Enable progress bar (based on
    plyr:::progress_text). Enabling the progress bar changes the
    \emph{default} value of \code{tasks} to \code{.Machine$integer.max}, so
    that progress is reported for each element of \code{X}.
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
  \item{exportglobals}{
    \code{logical(1)} Export \code{base::options()} from manager to
    workers? Default \code{TRUE}.
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
    call to \code{FUN}. The default (\code{TRUE}, explicitly
    call the garbage collection) can slow parallel computation, but is
    necessary when each call to \code{FUN} allocates a 'large' amount of
    memory. If \code{FUN} allocates little memory, then considerable
    performance improvements are gained by setting \code{force.GC = FALSE}.
  }
  \item{fallback}{
    \code{logical(1)} When \code{TRUE}, fall back to using
    \code{SerialParam} when \code{MulticoreParam} has not been started
    and the number of worker is no greater than 1.
  }
  \item{manager.hostname}{
    \code{character(1)} Host name of manager node. See 'Global Options',
    in \code{\link{SnowParam}}.
  }
  \item{manager.port}{
    \code{integer(1)} Port on manager with which workers
    communicate. See 'Global Options' in \code{\link{SnowParam}}.
  }
  \item{\dots}{
    Additional arguments passed to \code{\link{makeCluster}}
  }

}

\section{Accessors: Logging and results}{

  In the following code, \code{x} is a \code{MulticoreParam} object.
  \describe{
    \item{}{
      \code{bpprogressbar(x)}, \code{bpprogressbar(x) <- value}:
      Get or set the value to enable text progress bar.
      \code{value} must be a \code{logical(1)}.
    }
    \item{}{
      \code{bpjobname(x)}, \code{bpjobname(x) <- value}:
      Get or set the job name.
    }
    \item{}{
      \code{bpRNGseed(x)}, \code{bpRNGseed(x) <- value}:
      Get or set the seed for random number generaton. \code{value} must be a
      \code{numeric(1)} or \code{NULL}.
    }
    \item{}{
      \code{bplog(x)}, \code{bplog(x) <- value}:
      Get or set the value to enable logging. \code{value} must be a
      \code{logical(1)}.
    }
    \item{}{
      \code{bpthreshold(x)}, \code{bpthreshold(x) <- value}:
      Get or set the logging threshold. \code{value} must be a
      \code{character(1)} string of one of the levels defined in the
      \code{futile.logger} package: \dQuote{TRACE}, \dQuote{DEBUG},
      \dQuote{INFO}, \dQuote{WARN}, \dQuote{ERROR}, or \dQuote{FATAL}.
    }
    \item{}{
      \code{bplogdir(x)}, \code{bplogdir(x) <- value}:
      Get or set the directory for the log file. \code{value} must be a
      \code{character(1)} path, not a file name. The file is written out as
      LOGFILE.out. If no \code{logdir} is provided and \code{bplog=TRUE} log
      messages are sent to stdout.
    }
    \item{}{
      \code{bpresultdir(x)}, \code{bpresultdir(x) <- value}:
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
    \item{}{\code{bpworkers(x)}}
    \item{}{\code{bpnworkers(x)}}
    \item{}{\code{bptasks(x)}, \code{bptasks(x) <- value}}
    \item{}{\code{bpstart(x)}}
    \item{}{\code{bpstop(x)}}
    \item{}{\code{bpisup(x)}}
    \item{}{\code{bpbackend(x)}, \code{bpbackend(x) <- value}}
  }

}

\section{Accessors: Error Handling}{

  In the code below \code{x} is a \code{MulticoreParam} object. See the
  ?\code{BiocParallelParam} man page for details on these accessors.
  \describe{
    \item{}{\code{bpstopOnError(x)}, \code{bpstopOnError(x) <- value}}
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
  }

}

\section{Global Options}{

  See the 'Global Options' section of \code{\link{SnowParam}} for
  manager host name and port defaults.

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
## -----------------------------------------------------------------------
## Job configuration:
## -----------------------------------------------------------------------

## MulticoreParam supports shared memory computing. The object fields
## control the division of tasks, error handling, logging and
## result format.
bpparam <- MulticoreParam()
bpparam

## By default the param is created with the maximum available workers
## determined by multicoreWorkers().
multicoreWorkers()

## Fields are modified with accessors of the same name:
bplog(bpparam) <- TRUE
dir.create(resultdir <- tempfile())
bpresultdir(bpparam) <- resultdir
bpparam

## -----------------------------------------------------------------------
## Logging:
## -----------------------------------------------------------------------

## When 'log == TRUE' the workers use a custom script (in BiocParallel)
## that enables logging and access to other job statistics. Log messages
## are returned as each job completes rather than waiting for all to finish.

## In 'fun', a value of 'x = 1' will throw a warning, 'x = 2' is ok
## and 'x = 3' throws an error. Because 'x = 1' sleeps, the warning
## should return after the error.

X <- 1:3
fun <- function(x) {
    if (x == 1) {
        Sys.sleep(2)
        sqrt(-x)                   ## warning
        x
    } else if (x == 2) {
        x                          ## ok
    } else if (x == 3) {
        sqrt("FOO")                ## error
    }
}

## By default logging is off. Turn it on with the bplog()<- setter
## or by specifying 'log = TRUE' in the constructor.
bpparam <- MulticoreParam(3, log = TRUE, stop.on.error = FALSE)
res <- tryCatch({
    bplapply(X, fun, BPPARAM=bpparam)
}, error=identity)
res

## When a 'logdir' location is given the messages are redirected to a file:
\dontrun{
bplogdir(bpparam) <- tempdir()
bplapply(X, fun, BPPARAM = bpparam)
list.files(bplogdir(bpparam))
}

## -----------------------------------------------------------------------
## Managing results:
## -----------------------------------------------------------------------

## By default results are returned as a list. When 'resultdir' is given
## files are saved in the directory specified by job, e.g., 'TASK1.Rda',
## 'TASK2.Rda', etc.
\dontrun{
dir.create(resultdir <- tempfile())
bpparam <- MulticoreParam(2, resultdir = resultdir, stop.on.error = FALSE)
bplapply(X, fun, BPPARAM = bpparam)
list.files(bpresultdir(bpparam))
}

## -----------------------------------------------------------------------
## Error handling:
## -----------------------------------------------------------------------

## When 'stop.on.error' is TRUE the job is terminated as soon as an
## error is hit. When FALSE, all computations are attempted and partial
## results are returned along with errors. In this example the number of
## 'tasks' is set to equal the length of 'X' so each element is run
## separately. (Default behavior is to divide 'X' evenly over workers.)

## All results along with error:
bpparam <- MulticoreParam(2, tasks = 4, stop.on.error = FALSE)
res <- bptry(bplapply(list(1, "two", 3, 4), sqrt, BPPARAM = bpparam))
res

## Calling bpok() on the result list returns TRUE for elements with no error.
bpok(res)

## -----------------------------------------------------------------------
## Random number generation:
## -----------------------------------------------------------------------

## Random number generation is controlled with the 'RNGseed' field.
## This seed is passed to parallel::clusterSetRNGStream
## which uses the L'Ecuyer-CMRG random number generator and distributes
## streams to members of the cluster.

bpparam <- MulticoreParam(3, RNGseed = 7739465)
bplapply(seq_len(bpnworkers(bpparam)), function(i) rnorm(1), BPPARAM = bpparam)

}

\keyword{classes}
\keyword{methods}
