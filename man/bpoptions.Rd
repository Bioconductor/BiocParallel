\name{bpoptions}

\alias{bpoptions}

\title{Additional options to parallel evaluation}

\description{
    This function is used to pass the additional options to the apply function. One use case is to use the argument \code{BPOPTIONS} in the apply function to temporarily change the parameter of \code{BPPARAM}(e.g. enabling the progressbar). It can also be used to change the behavior of the parallel evaluation(e.g. manually exporting some variables to the worker).
}
\usage{
bpoptions(
        workers, tasks, jobname,
        log, logdir, threshold,
        resultdir, stop.on.error,
        timeout, exportglobals, exportvariables,
        progressbar, RNGseed, force.GC, fallback,
        exports, packages, ...)
}

\arguments{
  \item{workers}{A parameter for \code{BPPARAM}; see \code{\link{bpnworkers}}.}
  \item{tasks}{A parameter for \code{BPPARAM}; see \code{\link{bptasks}}.}
  \item{jobname}{A parameter for \code{BPPARAM}; see \code{\link{bpjobname}}.}
  \item{log}{A parameter for \code{BPPARAM}; see \code{\link{bplog}}.}
  \item{logdir}{A parameter for \code{BPPARAM}; see \code{\link{bplogdir}}.}
  \item{threshold}{A parameter for \code{BPPARAM}; see \code{\link{bpthreshold}}.}
  \item{resultdir}{A parameter for \code{BPPARAM}; see \code{\link{bpresultdir}}.}
  \item{stop.on.error}{A parameter for \code{BPPARAM}; see \code{\link{bpstopOnError}}.}
  \item{timeout}{A parameter for \code{BPPARAM}; see \code{\link{bptimeout}}.}
  \item{exportglobals}{A parameter for \code{BPPARAM}; see \code{\link{bpexportglobals}}.}
  \item{exportvariables}{A parameter for \code{BPPARAM}; see \code{\link{bpexportvariables}}.}
  \item{progressbar}{A parameter for \code{BPPARAM}; see \code{\link{bpprogressbar}}.}
  \item{RNGseed}{A parameter for \code{BPPARAM}; see \code{\link{bpRNGseed}}.}
  \item{force.GC}{A parameter for \code{BPPARAM}; see \code{\link{bpforceGC}}.}
  \item{fallback}{A parameter for \code{BPPARAM}; see \code{\link{bpfallback}}.}
  \item{exports}{The names of the variables in the global environment which needs to be exported to the global environment of the worker. This option works independently with the option \code{exportvariables}.}
  \item{packages}{The packages that needs to be attached by the worker prior to the evaluation of the task. This option works independently with the option \code{exportvariables}.}
  \item{...}{Additional arguments which may(or may not) work for some specific type of \code{BPPARAM}.}
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
