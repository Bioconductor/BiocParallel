\name{register}
\alias{register}
\alias{registered}
\alias{bpparam}

\title{Maintain a global registry of available back-end Params}

\description{

  Use functions on this page to add to or query a registry of back-ends,
  including the default for use when no \code{BPPARAM} object is
  provided to functions.

}

\usage{
register(BPPARAM, default=TRUE)
registered(bpparamClass)
bpparam(bpparamClass)
}

\arguments{

  \item{BPPARAM}{
    An instance of a \code{BiocParallelParam} class, e.g.,
    \code{\link{MulticoreParam}}, \code{\link{SnowParam}},
    \code{\link{DoparParam}}.
  }

  \item{default}{
    Make this the default \code{BiocParallelParam} for subsequent
    evaluations? If \code{FALSE}, the argument is placed at the lowest
    priority position.
  }

  \item{bpparamClass}{
    When present, the text name of the \code{BiocParallelParam} class
    (e.g., \dQuote{MulticoreParam}) to be retrieved from the
    registry. When absent, a list of all registered instances is
    returned.
  }
}

\details{

  The registry is a list of back-ends with configuration parameters 
  for parallel evaluation. The first list entry is the default and is 
  used by \code{BiocParallel} functions when no \code{BPPARAM} argument 
  is supplied.

  At load time the registry is populated with default backends. On Windows
  these are \code{SnowParam} and \code{SerialParam} and on non-Windows 
  \code{MulticoreParam}, \code{SnowParam} and \code{SerialParam}.
  When \code{snowWorkers()} or \code{multicoreWorkers} returns a single
  core, only \code{SerialParm} is registered.

  The \code{\link{BiocParallelParam}} objects are constructed from global
  options of the corresponding name, or from the default constructor (e.g.,
  \code{SnowParam()}) if no option is specified. The user can set customizations
  during start-up (e.g., in an \code{.Rprofile} file) with, for instance,
  \code{options(MulticoreParam=quote(MulticoreParam(workers=8)))}.

  The act of \dQuote{registering} a back-end modifies the existing
  \code{\link{BiocParallelParam}} in the list; only one param of each 
  type can be present in the registry. When \code{default=TRUE}, the 
  newly registered param is moved to the top of the list thereby making 
  it the default. When \code{default=FALSE}, the param is modified 
  `in place` vs being moved to the top.

  \code{bpparam()}, invoked with no arguments, returns the default 
  \code{\link{BiocParallelParam}} instance from the registry.
  When called with the text name of a \code{bpparamClass}, the 
  global options are consulted first,
  e.g., \code{options(MulticoreParam=MulticoreParam())} and then the
  value of \code{registered(bpparamClass)}.

}

\value{

  \code{register} returns, invisibly, a list of registered back-ends.

  \code{registered} returns the back-end of type \code{bpparamClass} or,
  if \code{bpparamClass} is missing,  a list of all registered back-ends.

  \code{bpparam} returns the back-end of type \code{bpparamClass} or,

}

\author{
  Martin Morgan \url{mailto:mtmorgan@fhcrc.org}.
}

\seealso{

  \code{\link{BiocParallelParam}} for possible values of \code{BPPARAM}.

}

\examples{

## ----------------------------------------------------------------------
## The registry 
## ----------------------------------------------------------------------

## The default registry.
default <- registered()
default

## When default = TRUE the last param registered becomes the new default.
snowparam <- SnowParam(workers = 3, type = "SOCK")
register(snowparam, default = TRUE)
registered()

## Retrieve the default back-end,
bpparam()

## or a specific BiocParallelParam.
bpparam("SnowParam")

## restore original registry -- push the defaults in reverse order
for (param in rev(default))
    register(param)

## ----------------------------------------------------------------------
## Specifying a back-end for evaluation
## ----------------------------------------------------------------------

## The back-end of choice is given as the BPPARAM argument to
## the BiocParallel functions. None, one, or multiple back-ends can be
## used.

bplapply(1:6, sqrt, BPPARAM = MulticoreParam(3))

## When not specified, the default from the registry is used.
bplapply(1:6, sqrt)

}

\keyword{manip}
