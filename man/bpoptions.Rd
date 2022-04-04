\name{bpoptions}

\alias{bpoptions}

\title{Additional options to parallel evaluation}

\description{
  This function is used to pass additional options to \code{bplapply()}
  and other functions function. One use case is to use the argument
  \code{BPOPTIONS} to temporarily change the parameter of \code{BPPARAM}
  (e.g. enabling the progressbar). A second use case is to change the
  behavior of the parallel evaluation (e.g. manually exporting some
  variables to the worker)
}
\usage{
bpoptions(
    workers, tasks, jobname, log, logdir, threshold, resultdir,
    stop.on.error, timeout, exportglobals, exportvariables, progressbar,
    RNGseed, force.GC, fallback, exports, packages, ...
)
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

  \item{exportvariables}{A parameter for \code{BPPARAM}; see
    \code{\link{bpexportvariables}}.}

  \item{progressbar}{logical(1) parameter for \code{BPPARAM}; see
    \code{\link{bpprogressbar}}.}

  \item{RNGseed}{integer(1) parameter for \code{BPPARAM}; see
    \code{\link{bpRNGseed}}.}

  \item{force.GC}{logical(1) parameter for \code{BPPARAM}; see
    \code{\link{bpforceGC}}.}

  \item{fallback}{logical(1) parameter for \code{BPPARAM}; see
    \code{\link{bpfallback}}.}

  \item{exports}{character() The names of the variables in the global
    environment which need to be exported to the global environment of
    the worker. This option works independently of the option
    \code{exportvariables}.}

  \item{packages}{character() The packages that needs to be attached by
    the worker prior to the evaluation of the task. This option works
    independently of the option \code{exportvariables}.}

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
