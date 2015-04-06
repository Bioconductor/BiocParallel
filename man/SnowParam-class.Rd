\name{SnowParam-class}
\Rdversion{1.1}
\docType{class}

% Class 
\alias{SnowParam}
\alias{SnowParam-class}

% Control
\alias{snowWorkers}
\alias{bpbackend,SnowParam-method}
\alias{bpbackend<-,SnowParam,cluster-method}
\alias{bpisup,SnowParam-method}
\alias{bpstart,SnowParam-method}
\alias{bpstop,SnowParam-method}
\alias{bpworkers,SnowParam-method}
\alias{bplog<-,SnowParam,logical-method}

% Accessors
\alias{bplog}
\alias{bplog<-}
\alias{bplog,SnowParam-method}
\alias{bplog<-,SnowParam,logical-method}
\alias{bpthreshold}
\alias{bpthreshold<-}
\alias{bpthreshold,SnowParam-method}
\alias{bpthreshold<-,SnowParam,character-method}
\alias{bplogdir}
\alias{bplogdir<-}
\alias{bplogdir,SnowParam-method}
\alias{bplogdir<-,SnowParam,character-method}
\alias{bpresultdir}
\alias{bpresultdir<-}
\alias{bpresultdir,SnowParam-method}
\alias{bpresultdir<-,SnowParam,character-method}

% Other methods
\alias{coerce,SOCKcluster,SnowParam-method}
\alias{coerce,spawnedMPIcluster,SnowParam-method}
\alias{show,SnowParam-method}

% Other functions
\alias{bpslaveLoop}
\alias{bprunMPIslave}
\alias{bpok}

\title{Enable simple network of workstations (SNOW)-style parallel evaluation}

\description{
  This class is used to parameterize simple network of workstations
  (SNOW) parallel evaluation on one or several physical computers.
  \code{snowWorkers()} chooses the number of workers based on global
  user preference (\code{options(mc.cores=...)}), or the minimum of 8
  and the number of detected cores (\code{detectCores()}).
}

\usage{
SnowParam(workers = snowWorkers(), type=c("SOCK", "MPI", "FORK"),
          catch.errors=TRUE, stop.on.error = FALSE, 
          log = FALSE, tasks = 0L, threshold = "INFO", logdir = character(), 
          resultdir = character(), ...)

## invoke as(cl, "SnowParam")
\S4method{coerce}{SOCKcluster,SnowParam}(from, to)
\S4method{coerce}{spawnedMPIcluster,SnowParam}(from, to)

snowWorkers()
}

\details{
  \code{SnowParam} is used for distributed memory computing and supports
  2 cluster types: \sQuote{SOCK} (default) and \sQuote{MPI}. The \code{SnowParam}
  builds on infrastructure in the \code{snow} and \code{parallel}
  packages and provides the additional features of error handling, logging
  and writing out results.

  \describe{
    \item{error handling:}{
      Two flags control error handling: \code{catch.errors} and
      \code{stop.on.error}.

      The \code{catch.errors} flag determines whether apply-like functions quit
      with an error as soon as one task fails. When \code{FALSE}, this is the
      behavior seen in the \code{parallel} and \code{snow} packages; an error is
      returned with no results.  When \code{TRUE}, errors are returned for tasks
      that failed as well as results for those that completed successfully.

      \code{stop.on.error} offers and additional level of control and is only
      applicable when \code{catch.errors == TRUE}. When this argument is
      \code{TRUE} the job terminates upon the first error is encountered but the
      results also contain the successfully completed tasks. \code{catch.errors}
      must be \code{TRUE} when \code{stop.on.error} is \code{TRUE}. When
      \code{FALSE}, the job runs to completion and all successfully completed
      tasks are returned along with any error messages.
    }
    \item{logging:}{
      Logging is implemented on the workers with the \code{futile.logger}
      package. When \code{bplog(BPPARAM) == TRUE} cluster workers use a script
      in the \code{BiocParallel} package instead of \code{snow}. This modified
      script captures warning and error messages and collects additional task
      statistics such as gc output and node name.
      
      Any \code{futile.logger} messages the user provides in their function will
      be captured by the logging mechanism. Messages are returned real-time
      (i.e., as each tasks completes) instead of after all jobs have finished.
    }
    \item{write log and result files:}{
      Results and logs can be written to a file instead of returned to
      the workspace. Writing to files is done from the master as each task 
      completes. These options are controlled with \code{logdir} 
      and \code{resultdir}. 
    }
  }

  NOTE: The \code{PSOCK} cluster from the \code{parallel} package does not 
  support cluster options \code{scriptdir} and \code{useRscript}. Because
  these options are needed to use an alternate worker script, \code{PSOCK} 
  is not supported in \code{SnowParam}.
}

\arguments{
  \item{workers}{
    \code{integer(1)} Number of workers. Defaults to all cores available as
    determined by \code{detectCores}. For a \code{SOCK} cluster \code{workers}
    can be a \code{character()} vector of host names.
  }
  \item{type}{
    \code{character(1)} Type of cluster to use. Possible values are
    \code{SOCK} (default) and \code{MPI}. Instead of \code{type=FORK} use
    \code{MulticoreParam}.
  }
  \item{tasks}{
    \code{integer(1)}. The number of tasks per job. \code{value} must be a 
    scalar integer >= 0L.
 
    We define a job as a single call to a function such as \code{bplapply}, 
    \code{bpmapply} etc. A task is the division of the \code{X} argument
    into chunks. When \code{tasks == 0} (default), \code{X} is divided by
    the number of workers. This approach distributes \code{X} in 
    (approximately) equal chunks.
 
    A \code{tasks} value of > 0 dictates the total number of tasks. Values
    can range from 1 (all of \code{X} to a single worker) to the length of 
    \code{X} (each element of \code{X} to a different worker). 
 
    When the length of \code{X} is less than the number of workers each
    element of \code{X} is sent to a worker and \code{tasks} is ignored.
 
    Currently used in \code{SnowParam} and \code{MulticoreParam} only. 
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
  \item{from}{
    A \code{SOCKcluster} or \code{spawnedMPIcluster} instance created
    with \code{makeCluster} in the \code{parallel} or \code{snow} package.
    Applicable to \code{coerce} methods only.
  } 
  \item{to}{
    \code{character(1)} \dQuote{SnowParam}. Applicable to \code{coerce}
    methods only.
  }
}

\section{Constructor}{
  \describe{
    \item{}{
      \code{SnowParam(workers = snowWorkers(), type=c("SOCK", "MPI"),
                      catch.errors = TRUE, stop.on.error = FALSE,
                      log = FALSE, threshold = "INFO", logdir = character(), 
                      resultdir = character(), ...)}:

        Return an object representing a SNOW cluster. The cluster is not
        created until \code{bpstart} is called. Named arguments in \code{...} 
        are passed to \code{makeCluster}.
    }
  }
}

\section{Accessors}{
  In the following code, \code{x} is a SnowParam object.

  \describe{
    \item{}{
      \code{bpcatchErrors(x, ...)}, \code{bpcatchErrors(x) <- value}: 
      Get or set the flag to determine if apply-like functions quit with an
      error as soon as one application fails.  See ?\code{SnowParam} and
      ?\code{BatchJobsParam} for details applicable to each class.
    }
    \item{}{
      \code{bpstopOnError(x, ...)}, \code{bpststopOnError(x) <- value}: 
      Get or set the value to enable stop on error. \code{value} must be a
      \code{logical(1)}.  When TRUE, the function returns immediately upon
      error; results for previously completed jobs are saved.
    }
    \item{}{
      \code{bptasks(x, ...)}, \code{bptasks(x) <- value}: 
      Get or set the number of tasks for a job. \code{value} must be a scalar
      integer >= 0L.
 
      In this documentation a job is defined as a single call to a function such
      as \code{bplapply}, \code{bpmapply} etc. A task is the division of the
      \code{X} argument into chunks. When \code{tasks == 0} (default), \code{X}
      is divided by the number of workers. This approach distributes \code{X} in
      (approximately) equal chunks.
 
      A \code{tasks} value of > 0 dictates the total number of tasks. Values
      can range from 1 (all of \code{X} to a single worker) to the length of 
      \code{X} (each element of \code{X} to a different worker). 
 
      When the length of \code{X} is less than the number of workers each
      element of \code{X} is sent to a worker and \code{tasks} is ignored.
 
      Currently used in \code{SnowParam} and \code{MulticoreParam} only. 
    }
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
 
  See the ?\code{bpcontrols} man page for full details on these accessors.
  \describe{
    \item{}{\code{bpworkers(x, ...)}}
    \item{}{\code{bpstart(x, ...)}}
    \item{}{\code{bpstop(x, ...)}}
    \item{}{\code{bpisup(x, ...)}}
    \item{}{\code{bpbackend(x, ...)}, \code{bpbackend(x) <- value}: 
            \code{value} must be on of \code{SOCK} or \code{MPI}.}
  }
}

\section{Other Methods}{
  \describe{
    \item{}{
      \code{show(x)}:
      Displays the \code{SnowParam} object. 
    }
    \item{}{
      \code{bpok(x)}:
      Returns a \code{logical()} vector: FALSE for any jobs that resulted in
      an error. \code{x} is the result list output by a \code{BiocParallel}
      function such as \code{bplapply} or \code{bpmapply}. 
    }
  }
}

\section{Coercion}{
  \describe{
    \item{}{
      \code{as(from, "SnowParam")}:
      Creates a \code{SnowParam} object from a \code{SOCKcluster} or
      \code{spawnedMPIcluster} object. Instances created in this way
      cannot be started or stopped.
    }
  }
}

\author{Martin Morgan and Valerie Obenchain.}

\seealso{
  \itemize{
    \item \code{register} for registering parameter classes for use in 
          parallel evaluation.

    \item \code{\link{MulticoreParam}} for computing in shared memory

    \item \code{\link{BatchJobsParam}} for computing with cluster schedulers

    \item \code{\link{SerialParam}} for non-parallel evaluation 
  }
}

\examples{

## -----------------------------------------------------------------------
## Job configuration:
## -----------------------------------------------------------------------

## SnowParam supports SOCK and  MPI clusters for parallel computing in 
## distributed memory. Both use methods from the 'snow' package with
## a custom worker script that enables logging and access to other
## job statistics.
bpparam <- SnowParam(3, type="SOCK")

## By default, logging is off.
bpparam

## -----------------------------------------------------------------------
## Logging:
## -----------------------------------------------------------------------

## Log messages are returned in 'real-time' as each job finishes instead
## of after all jobs have competed. The sleep time in 'fun' helps demonstrate
## this. Job 1 (X = 1) will throw a warning and job 3 (X = 3) an error. 
## Job 1 takes longer to complete so the error message is returned first, 
## followed by the warning.

X <- 1:3 
fun <- function(x) {
    if (x == 1) {
        Sys.sleep(2)
        if (TRUE & c(TRUE, TRUE))  ## warning
            x 
    } else if (x == 2) { 
        x                          ## ok
    } else if (x == 3) { 
        sqrt("FOO")                ## error
    }
}

## By default logging is off; turn it on with bplog():
bplog(bpparam) <- TRUE
bplapply(X, fun, BPPARAM=bpparam)

## When a 'logdir' location is given the log messages are redirected to a file:
bplogdir(bpparam) <- tempdir()
bplapply(X, fun, BPPARAM=bpparam)
list.files(bplogdir(bpparam))

## -----------------------------------------------------------------------
## Managing results:
## -----------------------------------------------------------------------

## Results can be written to a file by providing a 'resultdir' location.
bpparam <- SnowParam(2, type="SOCK", resultdir = tempdir())
bplapply(X, fun, BPPARAM=bpparam)

## -----------------------------------------------------------------------
## Error handling:
## -----------------------------------------------------------------------

## When 'stopOnError' is TRUE, the method returns as soon as an error
## is thrown.
bpparam <- SnowParam(2, type="SOCK", stop.on.error=TRUE)
res <- bplapply(list(1, "two", 3, 4), sqrt, BPPARAM=bpparam)
res

## FIXME: flag the NULL?
## bpok() returns TRUE for the elements that did not return an error.
bpok(res)

## When 'stopOnError' is FALSE, all results are computed.
bpstopOnError(bpparam) <- FALSE 
res <- bplapply(list(1, "two", 3, 4), sqrt, BPPARAM=bpparam)
bpok(res)
}

\keyword{classes}
\keyword{methods}
