\name{SerialParam-class}
\Rdversion{1.1}
\docType{class}
\alias{SerialParam-class}
\alias{SerialParam}

\alias{bpisup,SerialParam-method}
\alias{bpworkers,SerialParam-method}
\alias{bplog,SerialParam-method}
\alias{bplogdir,SerialParam-method}
\alias{bplog<-,SerialParam,logical-method}
\alias{bpthreshold<-,SerialParam,character-method}
\alias{bplogdir<-,SerialParam,character-method}

\title{Enable serial evaluation}

\description{

  This class is used to parameterize serial evaluation, primarily to
  facilitate easy transition from parallel to serial code.

}

\usage{
SerialParam(stop.on.error = TRUE, log = FALSE,
    threshold = "INFO", logdir = NA_character_, progressbar = FALSE)
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

  \item{stop.on.error}{A \code{logical} determining behavior on error;
    see \code{\link{SnowParam}}.}

  \item{log}{
    \code{logical(1)} Enable logging; see \code{\link{SnowParam}}.
  }

  \item{threshold}{
    \code{character(1)} Logging threshold; see \code{\link{SnowParam}}.
  }

  \item{logdir}{
    \code{character(1)} Log files directory. When not provided, log
    messages are returned to stdout.
  }

  \item{progressbar}{
    \code{logical(1)} Enable progress bar (based on plyr:::progress_text).
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

\dontrun{
register(SerialParam(), default=TRUE)
}
}

\keyword{classes}
