\name{BiocParallelParam-class}
\Rdversion{1.1}
\docType{class}

% Class
\alias{BiocParallelParam-class}
\alias{BiocParallelParam}

% Control
\alias{bpbackend}
\alias{bpbackend<-}
\alias{bpbackend,missing-method}
\alias{bpbackend<-,missing,ANY-method}
\alias{bpisup}
\alias{bpisup,ANY-method}
\alias{bpisup,missing-method}
\alias{bpstart}
\alias{bpstart,ANY-method}
\alias{bpstart,missing-method}
\alias{bpstart,BiocParallelParam-method}
\alias{bpstop}
\alias{bpstop,ANY-method}
\alias{bpstop,missing-method}
\alias{bpstop,BiocParallelParam-method}
\alias{bpnworkers}
\alias{bpworkers}
\alias{bpworkers<-}
\alias{bpworkers,missing-method}
\alias{bpworkers,BiocParallelParam-method}
\alias{bptasks}
\alias{bptasks,BiocParallelParam-method}
\alias{bptasks<-}
\alias{bptasks<-,BiocParallelParam-method}
\alias{bptasks<-,BiocParallelParam,ANY-method}
\alias{bpstopOnError}
\alias{bpstopOnError,BiocParallelParam-method}
\alias{bpstopOnError<-}
\alias{bpstopOnError<-,BiocParallelParam,logical-method}
\alias{bpstopOnError<-,DoparParam,logical-method}
\alias{bplog}
\alias{bplog<-}
\alias{bplog,BiocParallelParam-method}
\alias{bpthreshold}
\alias{bpthreshold<-}
\alias{bpthreshold,BiocParallelParam-method}
\alias{bplogdir}
\alias{bplogdir<-}
\alias{bplogdir,BiocParallelParam-method}
\alias{bplogdir<-,BiocParallelParam,character-method}
\alias{bpresultdir}
\alias{bpresultdir<-}
\alias{bpresultdir,BiocParallelParam-method}
\alias{bpresultdir<-,BiocParallelParam,character-method}
\alias{bptimeout}
\alias{bptimeout<-}
\alias{bptimeout,BiocParallelParam-method}
\alias{bptimeout<-,BiocParallelParam,numeric-method}
\alias{bpexportglobals}
\alias{bpexportglobals<-}
\alias{bpexportglobals,BiocParallelParam-method}
\alias{bpexportglobals<-,BiocParallelParam,logical-method}
\alias{bpexportvariables}
\alias{bpexportvariables<-}
\alias{bpexportvariables,BiocParallelParam-method}
\alias{bpexportvariables<-,BiocParallelParam,logical-method}
\alias{bpprogressbar}
\alias{bpprogressbar,BiocParallelParam-method}
\alias{bpprogressbar<-}
\alias{bpprogressbar<-,BiocParallelParam,logical-method}
\alias{bpjobname}
\alias{bpjobname,BiocParallelParam-method}
\alias{bpjobname<-}
\alias{bpjobname<-,BiocParallelParam,character-method}
\alias{bpRNGseed}
\alias{bpRNGseed<-}
\alias{bpRNGseed,BiocParallelParam-method}
\alias{bpRNGseed<-,BiocParallelParam,NULL-method}
\alias{bpRNGseed<-,BiocParallelParam,numeric-method}
\alias{bpforceGC}
\alias{bpforceGC,BiocParallelParam-method}
\alias{bpforceGC<-}
\alias{bpforceGC<-,BiocParallelParam,numeric-method}
\alias{bpfallback}
\alias{bpfallback,BiocParallelParam-method}
\alias{bpfallback<-}
\alias{bpfallback<-,BiocParallelParam,logical-method}

% Other methods
\alias{show,BiocParallel-method}
\alias{print.remote_error}


\title{BiocParallelParam objects}

\description{

  The \code{BiocParallelParam} virtual class stores configuration parameters
  for parallel execution. Concrete subclasses include \code{SnowParam},
  \code{MulticoreParam}, \code{BatchtoolsParam}, and \code{DoparParam}
  and \code{SerialParam}.

}

\details{

  \code{BiocParallelParam} is the virtual base class on which other
  parameter objects build. There are 5 concrete subclasses:

  \describe{
      \item{\code{SnowParam}:}{distributed memory computing}
      \item{\code{MulticoreParam}:}{shared memory computing}
      \item{\code{BatchtoolsParam}:}{scheduled cluster computing}
      \item{\code{DoparParam}:}{foreach computing}
      \item{\code{SerialParam}:}{non-parallel execution}
  }

  The parameter objects hold configuration parameters related to the
  method of parallel execution such as shared memory, independent
  memory or computing with a cluster scheduler.

}

\section{Construction}{

  The \code{BiocParallelParam} class is virtual and has no constructor.
  Instances of the subclasses can be created with the following:

  \itemize{
    \item \code{SnowParam()}
    \item \code{MulticoreParam()}
    \item \code{BatchtoolsParam()}
    \item \code{DoparParam()}
    \item \code{SerialParam()}
  }

}

\section{Accessors}{

  \subsection{Back-end control}{
    In the code below \code{BPPARAM} is a \code{BiocParallelParam} object.

    \describe{
      \item{\code{bpworkers(x)}, \code{bpworkers(x, ...)}:}{
        \code{integer(1)} or \code{character()}. Gets the number or names of
        the back-end workers. The setter is supported for SnowParam and
        MulticoreParam only.
      }
      \item{\code{bpnworkers(x)}:}{
        \code{integer(1)}. Gets the number of the back-end workers.
      }
      \item{\code{bptasks(x)}, \code{bptasks(x) <- value}:}{
        \code{integer(1)}. Get or set the number of tasks for a
        job. \code{value} can be a scalar integer > 0L, or integer 0L for
        matching the worker number, or \code{NA} for representing an infinite
        task number.
        \code{DoparParam} and \code{BatchtoolsParam} have their own
        approach to dividing a job among workers.

        We define a job as a single call to a function such as \code{bplapply},
        \code{bpmapply} etc. A task is the division of the
        \code{X} argument into chunks. When \code{tasks == 0} (default),
        \code{X} is divided by the number of workers. This approach distributes
        \code{X} in (approximately) equal chunks.

        A \code{tasks} value of > 0 dictates the total number of
        tasks. Values can range from 1 (all of \code{X} to a single
        worker) to the length of \code{X} (each element of \code{X}
        to a different worker); values greater than \code{length(X)}
        (e.g., \code{.Machine$integer.max}) are rounded to \code{length(X)}.

        When the length of \code{X} is less than the number of workers each
        element of \code{X} is sent to a worker and \code{tasks} is ignored.
        Another case where the \code{tasks} value is ignored is when using the
        \code{bpiterate} function; the number of tasks are defined by the number
        of data chunks returned by the \code{ITER} function.
      }
      \item{\code{bpstart(x)}:}{
        \code{logical(1)}. Starts the back-end, if necessary.
      }
      \item{\code{bpstop(x)}:}{
        \code{logical(1)}. Stops the back-end, if necessary and possible.
      }
      \item{\code{bpisup(x)}:}{
        \code{logical(1)}. Tests whether the back-end is available for
        processing, returning a scalar logical value. \code{bp*}
        functions such as \code{bplapply} automatically start the
        back-end if necessary.
      }
      \item{\code{bpbackend(x)}, \code{bpbackend(x) <- value}:}{
        Gets or sets the parallel \code{bpbackend}. Not all back-ends can
        be retrieved; see \code{methods("bpbackend")}.
      }
      \item{\code{bplog(x)}, \code{bplog(x) <- value}:}{
        Get or enable logging, if available.  \code{value} must be a
        \code{logical(1)}.
      }
      \item{\code{bpthreshold(x)}, \code{bpthreshold(x) <- value}:}{
        Get or set the logging threshold. \code{value} must be a
        \code{character(1)} string of one of the levels defined in the
        \code{futile.logger} package: \dQuote{TRACE}, \dQuote{DEBUG},
        \dQuote{INFO}, \dQuote{WARN}, \dQuote{ERROR}, or \dQuote{FATAL}.
      }
      \item{\code{bplogdir(x)}, \code{bplogdir(x) <- value}:}{
        Get or set an optional directory for saving log files. The
        directory must already exist with read / write ability.
      }
      \item{\code{bpresultdir(x)}, \code{bpresultdir(x) <- value}:}{
        Get or set an optional directory for saving results as 'rda'
        files. The directory must already exist with read / write
        ability.
      }
      \item{\code{bptimeout(x)}, \code{bptimeout(x) <- value}:}{
        \code{numeric(1)} Time (in seconds) allowed for worker to
        complete a task.  This value is passed to base::setTimeLimit()
        as both the \code{cpu} and \code{elapsed} arguments. If the
        computation exceeds \code{timeout} an error is thrown with
        message 'reached elapsed time limit'.
      }
      \item{\code{bpexportglobals(x)}, \code{bpexportglobals(x) <- value}:}{
        \code{logical(1)} Export \code{base::options()} from manager to
        workers? Default \code{TRUE}.
      }
      \item{\code{bpexportvariables(x)}, \code{bpexportvariables(x) <- value}:}{
        \code{logical(1)} Automatically export the variables which are
        defined in the global environment and used by the function from
        manager to workers. Default \code{TRUE}.
      }
      \item{\code{bpprogressbar(x)}, \code{bpprogressbar(x) <- value}:}{
        Get or set the value to enable text progress bar.
        \code{value} must be a \code{logical(1)}.
      }
      \item{\code{bpRNGseed(x)}, \code{bpRNGseed(x) <- value}:}{
        Get or set the seed for random number generaton. \code{value} must be a
        \code{numeric(1)} or \code{NULL}.
      }
      \item{\code{bpjobname(x)}, \code{bpjobname(x) <- value}:}{
        Get or set the job name.
      }
      \item{\code{bpforceGC(x)}, \code{bpforceGC(x) <- value}:}{
        Get or set whether 'garbage collection' should be invoked at the
        end of each call to \code{FUN}.
      }
      \item{\code{bpfallback(x)}, \code{bpfallback(x) <- value}:}{
        Get or set whether the fallback \code{SerialParam} should be
        used (e.g., for efficiency when starting a cluster) when the
        current \code{BPPARAM} has not been started and the worker
        number is less than or equal to 1.
      }
    }
  }

  \subsection{Error Handling}{
    In the code below \code{BPPARAM} is a \code{BiocParallelParam} object.

    \describe{
      \item{\code{bpstopOnError(x)}, \code{bpstopOnError(x) <- value}:}{
        \code{logical()}. Controls if the job stops when an error is hit.

        \code{stop.on.error} controls whether the job stops after an
        error is thrown. When \code{TRUE}, the output contains all
        successfully completed results up to and including the error.
        When \code{stop.on.error == TRUE} all computations stop once the
        error is hit. When \code{FALSE}, the job runs to completion
        and successful results are returned along with any error messages.
      }
    }
  }

}

\section{Methods}{

  \subsection{Evaluation}{
    In the code below \code{BPPARAM} is a \code{BiocParallelParam} object.
    Full documentation for these functions are on separate man pages: see
    ?\code{bpmapply}, ?\code{bplapply}, ?\code{bpvec}, ?\code{bpiterate} and
    ?\code{bpaggregate}.

    \itemize{

      \item \code{bpmapply(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE,
        USE.NAMES=TRUE, BPPARAM=bpparam())}

      \item \code{bplapply(X, FUN, ..., BPPARAM=bpparam())}

      \item \code{bpvec(X, FUN, ..., AGGREGATE=c, BPPARAM=bpparam())}

      \item \code{bpiterate(ITER, FUN, ..., BPPARAM=bpparam())}

      \item \code{bpaggregate(x, data, FUN, ..., BPPARAM=bpparam())}
    }
  }

  \subsection{Other}{
    In the code below \code{BPPARAM} is a \code{BiocParallelParam} object.

    \itemize{
      \item \code{show(x)}
    }
  }

}

\author{Martin Morgan and Valerie Obenchain.}

\seealso{

  \itemize{
    \item \code{\link{SnowParam}} for computing in distributed memory

    \item \code{\link{MulticoreParam}} for computing in shared memory

    \item \code{\link{BatchtoolsParam}} for computing with cluster schedulers

    \item \code{\link{DoparParam}} for computing with foreach

    \item \code{\link{SerialParam}} for non-parallel execution
  }

}

\examples{

getClass("BiocParallelParam")

## For examples see ?SnowParam, ?MulticoreParam, ?BatchtoolsParam
## and ?SerialParam.
}

\keyword{classes}
\keyword{methods}
