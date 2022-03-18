\name{bpoptions}

\alias{bpoptions}

\title{Additional options to parallel evaluation}

\description{
  This function is used to pass additional options to \code{bplapply()}
  and other functions function. One use case is to use the argument
  \code{BPOPTIONS} to temporarily change the parameter of \code{BPPARAM}
  (e.g. enabling the progressbar). A second use case is to change the
  behavior of the parallel evaluation (e.g. disabling garbage collection
  on the worker)
}
\usage{
bpoptions(
        workers, tasks, jobname,
        log, logdir, threshold,
        resultdir, stop.on.error,
        timeout, exportglobals, progressbar,
        RNGseed, force.GC, fallback, ...)
}

\arguments{
  \item{workers}{integer(1) or character() parameter for \code{BPPARAM};
    see \code{\link{bpnworkers}}.}

  \item{tasks}{integer(1) parameter for \code{BPPARAM}; see
    \code{\link{bptasks}}.}

  \item{jobname}{character(1) parameter for \code{BPPARAM}; see
    \code{\link{bpjobname}}.}

  \item{log}{logical(1) parameter for \code{BPPARAM}; see
    \code{\link{bplog}}.}

  \item{logdir}{character(1) parameter for \code{BPPARAM}; see
    \code{\link{bplogdir}}.}

  \item{threshold}{ A parameter for \code{BPPARAM}; see
    \code{\link{bpthreshold}}.}

  \item{resultdir}{character(1) parameter for \code{BPPARAM}; see
    \code{\link{bpresultdir}}.}

  \item{stop.on.error}{logical(1) parameter for \code{BPPARAM}; see
    \code{\link{bpstopOnError}}.}

  \item{timeout}{integer(1) parameter for \code{BPPARAM}; see
    \code{\link{bptimeout}}.}

  \item{exportglobals}{logical(1) parameter for \code{BPPARAM}; see
    \code{\link{bpexportglobals}}.}

  \item{progressbar}{logical(1) parameter for \code{BPPARAM}; see
    \code{\link{bpprogressbar}}.}

  \item{RNGseed}{integer(1) parameter for \code{BPPARAM}; see
    \code{\link{bpRNGseed}}.}

  \item{force.GC}{logical(1) parameter for \code{BPPARAM}; see
    \code{\link{bpforceGC}}.}

  \item{fallback}{logical(1) parameter for \code{BPPARAM}; see
    \code{\link{bpfallback}}.}

  \item{...}{
    Additional arguments which may(or may not) work for some specific
    type of \code{BPPARAM}.
  }

}

\value{
  A list of options
}

\author{Jiefei Wang}

\seealso{
  \code{\link{BiocParallelParam}}, \code{\link{bplapply}}, \code{\link{bpiterate}}.
}
\examples{
  p <- SerialParam()
  bplapply(1:5, function(x) Sys.sleep(1), BPPARAM = p,
         BPOPTIONS = bpoptions(progressbar = TRUE, tasks = 5L))
}

\keyword{manip}
