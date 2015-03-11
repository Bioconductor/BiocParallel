\name{BiocParallelParam-class}
\Rdversion{1.1}
\docType{class}

% BiocParallelParam class, functions and methods
\alias{BiocParallelParam-class}
\alias{BiocParallelParam}

% Accessors
\alias{bplog}
\alias{bplog<-}
\alias{bplog,BiocParallelParam-method}
\alias{bplog<-,BiocParallelParam,logical-method}
\alias{bpthreshold}
\alias{bpthreshold<-}
\alias{bpthreshold,BiocParallelParam-method}
\alias{bpthreshold<-,BiocParallelParam,character-method}
\alias{bplogdir}
\alias{bplogdir<-}
\alias{bplogdir,BiocParallelParam-method}
\alias{bplogdir<-,BiocParallelParam,character-method}
\alias{bpresultdir}
\alias{bpresultdir<-}
\alias{bpresultdir,BiocParallelParam-method}
\alias{bpresultdir<-,BiocParallelParam,character-method}
\alias{bpstopOnError}
\alias{bpstopOnError<-}
\alias{bpstopOnError,BiocParallelParam-method}
\alias{bpstopOnError<-,BiocParallelParam,logical-method}

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
\alias{bpstop}
\alias{bpstop,ANY-method}
\alias{bpstop,missing-method}
\alias{bpworkers}
\alias{bpworkers,missing-method}
\alias{bpworkers,BiocParallelParam-method}

% Other methods
\alias{show,BiocParallel-method}


\title{BiocParallelParam objects}

\description{
  The \code{BiocParallelParam} virtual class stores configuration parameters
  for parallel execution. Concrete subclasses include \code{SnowParam}, 
  \code{MulticoreParam}, \code{BatchJobsParam}, and \code{DoparParam}
  and \code{SerialParam}.
}

\details{
  \code{BiocParallelParam} is the virtual base class on which other
  parameter objects build. There are 5 concrete subclasses:

  \describe{
      \item{}{\code{SnowParam}: distributed memory computing}
      \item{}{\code{MulticoreParam}: shared memory computing}
      \item{}{\code{BatchJobsParam}: scheduled cluster computing}
      \item{}{\code{DoparParam}: foreach computing}
      \item{}{\code{SerialParam}: non-parallel execution}
  }

  The parameter objects hold configuration parameters related to the 
  method of parallel execution such as shared memory, independent
  memory or computing with a cluster scheduler.
}

\section{Construction}{
  The \code{BiocParallelParam} class is virtual and has no constructor. 
  Instances of the subclasses can be created with the following constructors:

  \itemize{
    \item \code{SnowParam()}
    \item \code{MulticoreParam()}
    \item \code{BatchJobsParam()}
    \item \code{SerialParam()}
  }
}

\section{Accessors}{
  In the following code, \code{x} is a BiocParallelParam object.

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
}

\section{Methods: Evaluation}{
  In the code below \code{BPPARAM} is a \code{BiocParallelParam} object.
  Full documentation for these functions are on separate man pages: see
  ?\code{bpmapply}, ?\code{bplapply}, ?\code{bpvec}, ?\code{bpiterate} and
  ?\code{bpaggregate}.

  \describe{
    \item{}{
      \code{bpmapply(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
                     BPRESUME=getOption("BiocParallel.BPRESUME", FALSE), 
                     BPPARAM=bpparam())}
    }
    \item{}{
      \code{bplapply(X, FUN, ..., 
                     BPRESUME = getOption("BiocParallel.BPRESUME", FALSE),
                     BPPARAM=bpparam())}
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
    \item{}{
      \code{show(x)}
    }
  }
}

\section{Methods: Back-end control}{
  In the code below \code{BPPARAM} is a \code{BiocParallelParam} object.

  \describe{
    \item{}{
      \code{bpworkers(x, ...)}:
      Reports the number of workers in the back-end as a scalar integer with 
      value >= 0.
    }
    \item{}{
      \code{bpstart(x, ...)}:
      Starts the back-end, if necessary. For instance,
    }
    \item{}{
      \code{bpstop(x, ...)}:
      Stops the back-end, if necessary and possible.
    }
    \item{}{
      \code{bpisup(x, ...)}:
      Tests whether the back-end is available for processing, returning a 
      scalar logical value. \code{bp*} functions such as \code{bplapply} 
      automatically start the back-end if necessary.
    }
    \item{}{
      \code{bpbackend(x, ...)}, \code{bpbackend(x) <- value}: 
      Gets or sets the parallel \code{bpbackend}. Not all back-ends can
      be retrieved; see \code{showMethods("backend")}.
    }
  }
}

\author{Martin Morgan and Valerie Obenchain.}

\seealso{
  \itemize{
    \item \code{\link{SnowParam}} for computing in distributed memory

    \item \code{\link{MulticoreParam}} for computing in shared memory

    \item \code{\link{BatchJobsParam}} for computing with cluster schedulers

    \item \code{\link{DoparParam}} for computing with foreach

    \item \code{\link{SerialParam}} for non-parallel execution
  }
}

\examples{

getClass("BiocParallelParam")

## For examples see ?SnowParam, ?MulticoreParam, ?BatchJobsParam 
## and ?SerialParam.
}

\keyword{classes}
\keyword{methods}
