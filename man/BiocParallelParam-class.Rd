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
\alias{bpstop}
\alias{bpstop,ANY-method}
\alias{bpstop,missing-method}
\alias{bpworkers}
\alias{bpworkers<-}
\alias{bpworkers,missing-method}
\alias{bpworkers,BiocParallelParam-method}
\alias{bptasks}
\alias{bptasks,BiocParallelParam-method}
\alias{bptasks<-}
\alias{bptasks<-,BiocParallelParam,numeric-method}
\alias{bpcatchErrors}
\alias{bpcatchErrors,BiocParallelParam-method}
\alias{bpcatchErrors<-}
\alias{bpcatchErrors<-,BiocParallelParam,logical-method}
\alias{bpstopOnError}
\alias{bpstopOnError,BiocParallelParam-method}
\alias{bpstopOnError<-}
\alias{bpstopOnError<-,BiocParallelParam,logical-method}
\alias{bpstopOnError<-,DoparParam,logical-method}
\alias{bpprogressbar}
\alias{bpprogressbar,BiocParallelParam-method}
\alias{bpprogressbar<-}
\alias{bpprogressbar<-,BiocParallelParam,logical-method}

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
  Instances of the subclasses can be created with the following:

  \itemize{
    \item \code{SnowParam()}
    \item \code{MulticoreParam()}
    \item \code{BatchJobsParam()}
    \item \code{DoparParam()}
    \item \code{SerialParam()}
  }

}

\section{Accessors}{

  \subsection{Back-end control}{
    In the code below \code{BPPARAM} is a \code{BiocParallelParam} object.

    \describe{
      \item{}{
        \code{bpworkers(x, ...)}, \code{bpworkers(x, ...)}:
        \code{integer(1)} or \code{character()}. Gets the number or names of 
        the back-end workers. The setter is supported for SnowParam and 
        MulticoreParam only.
      }
      \item{}{
        \code{bptasks(x, ...)}, \code{bptasks(x) <- value}:
        \code{integer(1)}. Get or set the number of tasks for a job. \code{value}
        must be a scalar integer >= 0L. This argument applies
        to \code{SnowParam} and \code{MulticoreParam} only; \code{DoparParam}
        and \code{BatchJobsParam} have their own approach to dividing a job
        among workers. 
 
        We define a job as a single call to a function such as \code{bplapply}, 
        \code{bpmapply} etc. A task is the division of the
        \code{X} argument into chunks. When \code{tasks == 0} (default),
        \code{X} is divided by the number of workers. This approach distributes
        \code{X} in (approximately) equal chunks.
 
        A \code{tasks} value of > 0 dictates the total number of tasks. Values
        can range from 1 (all of \code{X} to a single worker) to the length of
        \code{X} (each element of \code{X} to a different worker). 
 
        When the length of \code{X} is less than the number of workers each
        element of \code{X} is sent to a worker and \code{tasks} is ignored.
        Another case where the \code{tasks} value is ignored is when using the
        \code{bpiterate} function; the number of tasks are defined by the number
        of data chunks returned by the \code{ITER} function. 
      }
      \item{}{
        \code{bpstart(x, ...)}:
        \code{logical(1)}. Starts the back-end, if necessary.
      }
      \item{}{
        \code{bpstop(x, ...)}:
        \code{logical(1)}. Stops the back-end, if necessary and possible.
      }
      \item{}{
        \code{bpisup(x, ...)}:
        \code{logical(1)}. Tests whether the back-end is available for processing,
        returning a scalar logical value. \code{bp*} functions such as
        \code{bplapply} automatically start the back-end if necessary.
      }
      \item{}{
        \code{bpbackend(x, ...)}, \code{bpbackend(x) <- value}: 
        Gets or sets the parallel \code{bpbackend}. Not all back-ends can
        be retrieved; see \code{showMethods("backend")}.
      }
      \item{progressbar}{
        \code{logical(1)} Enable progress bar. Applicable to
        \code{BatchJobsParam}, \code{SnowParam} and \code{MulticoreParam} only.
      }
    }
  }

  \subsection{Error Handling}{
    In the code below \code{BPPARAM} is a \code{BiocParallelParam} object.

    \describe{
      \item{}{
        \code{bpcatchErrors(x, ...)}, \code{bpCatchErrors(x) <- value}:
        \code{logical()}. Controls if errors are caught and returned with
        completed results.

        \code{catch.errors} determines whether errors are caught and
        returned with other results. When \code{TRUE}, all computations 
        are attempted and output contains both errors and successfully 
        completed results. When \code{FALSE}, the job is terminated as 
        soon as the first error is hit and only the error message is 
        returned (no results); this is the default behavior of the 
        parent packages, e.g., \code{parallel}, \code{snow}, 
        \code{foreach}.
      }
      \item{}{
        \code{bpstopOnError(x, ...)}, \code{bpstopOnError(x) <- value}:
        \code{logical()}. Controls if the job stops when an error is hit.

        \code{stop.on.error} controls whether the job stops after an 
        error is thrown. When \code{TRUE}, the output contains all 
        successfully completed results up to and including the error. 
        Unlike \code{catch.errors == TRUE}, when 
        \code{stop.on.error == TRUE} all computations stop once the 
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

    \describe{
      \item{}{
        \code{bpmapply(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
                       BPPARAM=bpparam())}
      }
      \item{}{
        \code{bplapply(X, FUN, ..., 
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
    }
  }

  \subsection{Other}{
    In the code below \code{BPPARAM} is a \code{BiocParallelParam} object.

    \describe{
      \item{}{
        \code{show(x)}
      }
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
