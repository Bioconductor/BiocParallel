\name{mclapply}
\alias{mclapply}
\title{Bioconductor mclapply, lapply across multiple cores (Unix-alike only)}

\description{
  This modifies the function \code{\link[parallel]{mclapply}} in the
  \code{parallel} package. The modification allows use of any object
  \code{X} supporting \code{length}, \code{[}, and \code{[[} methods.
}

\usage{
mclapply(X, FUN, ..., mc.preschedule = TRUE, mc.set.seed = TRUE, mc.silent = FALSE, mc.cores = getOption("mc.cores", 2L), mc.cleanup = TRUE, mc.allow.recursive = TRUE)
}

\arguments{
  \item{X}{
    Any list- or vector-like object for which methods \code{length},
    \code{[}, and \code{[[} are implemented.
  }
  \item{FUN}{{}}
  \item{\dots}{{}}
  \item{mc.preschedule}{{}}
  \item{mc.set.seed}{{}}
  \item{mc.silent}{{}}
  \item{mc.cores}{{}}
  \item{mc.cleanup}{{}}
  \item{mc.allow.recursive}{
    These arguments are as documented in
    \code{\link[parallel]{mclapply}} in the \code{parallel} package.
  }
}

\value{See \code{\link[parallel]{mclapply}}.}

\author{
  Martin Morgan \url{mailto:mtmorgan@fhcrc.org}. Original code as
  attributed in \code{\link[parallel]{pvec}}.
}

\seealso{
  \code{\link{pvec}} for parallel, vectorized
  calculations. \code{\link{parLapply}} for parallel lapply across
  separate processes.
}

\examples{
if (.Platform$OS.type != "windows") {
    ## ten tasks (1:10) so ten calls to FUN
    ## compare with pvec
    system.time(result <- mclapply(1:10, function(v) {
        message("working") ## 10 tasks
        sqrt(v)
    }))
    result
}}

\keyword{manip}
