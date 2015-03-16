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
\alias{bpstopOnError}
\alias{bpstopOnError<-}
\alias{bpstopOnError,SnowParam-method}
\alias{bpstopOnError<-,SnowParam,logical-method}

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
          stopOnError = FALSE, log = FALSE, threshold = "INFO",
          logdir = character(), resultdir = character(), ...)

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
      When \code{bpstopOnError(BPPARAM) == TRUE} the job will stop when
      an error is encountered. The return object includes results for
      completed tasks (i.e., complete before the error) and NULL for 
      tasks incomplete at the time of the error. When FALSE, all jobs are
      attempted and the result list contains all successfully completed
      tasks.
    }
    \item{logging:}{
      Logging is implemented on the workers with the \code{futile.logger}
      package. When \code{bplog(BPPARAM) == TRUE} cluster workers use a script
      in the \code{BiocParallel} package instead of \code{snow}. This modified
      script captures warning and error messages and collects additional task
      statistics such as gc output and node name. Log messages in a
      user-supplied function are also be captured by the logging mechanism.
      Messages are returned real-time (i.e., as each tasks completes) instead of
      after all jobs have finished.
    }
    \item{write log and result files:}{
      Results and logs can be written to a file instead of returned to
      the workspace. Writing to files is done real-time from the master 
      as each task completes. These options are controlled with \code{logdir} 
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
  \item{stopOnError}{
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
                      stopOnError = FALSE, log = FALSE, threshold = "INFO",
                      logdir = character(), resultdir = character(), ...)}:

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
      \code{bpstopOnError(x, ...)}, \code{bpststopOnError(x) <- value}: 
      Get or set the value to enable stop on error. \code{value} must be a
      \code{logical(1)}.  When TRUE, the function returns immediately upon
      error; results for previously completed jobs are saved.
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

## By default, logging is off (log=FALSE) and results are returned
## as an R object instead of written to a file (resultdir=character()).
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

## When a 'logdir' is given the output is redirected to a file:
bplogdir(bpparam) <- tempdir()
bplapply(X, fun, BPPARAM=bpparam)
list.files(bplogdir(bpparam))

## -----------------------------------------------------------------------
## Managing results:
## -----------------------------------------------------------------------

## By default results are returned as an R object (list). They can be 
## written to a file by providing a 'resultdir' location.
bpparam <- SnowParam(2, type="SOCK", resultdir = tempdir())
bplapply(X, fun, BPPARAM=bpparam)

## -----------------------------------------------------------------------
## Error handling:
## -----------------------------------------------------------------------

## When 'stopOnError' is TRUE, the method returns as soon as an error
## is thrown. The return list contains all results compelted prior to
## the error.
bpparam <- SnowParam(2, type="SOCK", stopOnError=TRUE)
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
