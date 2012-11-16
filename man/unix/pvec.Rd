\name{pvec}
\alias{pvec}
\title{Bioconductor pvec, parallel vectorization across multiple cores
  (Unix-alike only)}

\description{
  This modifies the function \code{\link[parallel]{pvec}} in the
  \code{parallel} package. The modification allows use of any object
  \code{v} supporting \code{length}, \code{[}, and \code{[[} methods.
}

\usage{
pvec(v, FUN, ..., mc.set.seed = TRUE, mc.silent = FALSE,
     mc.cores = getOption("mc.cores", 2L), mc.cleanup = TRUE,
     mc.preschedule=FALSE, mc.num.chunks, mc.chunk.size)
}

\arguments{
  \item{v}{
    Any list- or vector-like object for which methods \code{length},
    \code{[}, and \code{[[} are implemented. Note that \code{FUN} is
    expected to operate on a vector \code{v}.
  }

  \item{FUN}{{}}
  \item{\dots}{{}}
  \item{mc.set.seed}{{}}
  \item{mc.silent}{{}}
  \item{mc.cores}{{}}
  \item{mc.cleanup}{
    These arguments are as documented in
    \code{\link[parallel]{pvec}} in the \code{parallel} package.
  }
  \item{mc.preschedule}{
    This argument is as documented in
    \code{\link[parallel]{mclapply}} in the \code{parallel} package.
  }
  \item{num.chunks}{
    Specify the exact number of chunks to split the input into. The
    default is to make exactly one chunk per core. If supplied
    explicitly, this option takes precedence over \code{chunk.size}.
  }
  \item{chunk.size}{
    Size of chunks to split the input into. Note that not all chunks
    will be exactly the specified size unless the chunk size exactly
    divides the length of the input.
  }
}

\value{See \code{\link[parallel]{pvec}}.}

\author{
  Martin Morgan \url{mailto:mtmorgan@fhcrc.org}. Original code as
  attributed in \code{\link[parallel]{pvec}}.
}

\seealso{
  \code{\link{mclapply}} for parallel, lapply on muliple cores
  (Unix-alike only). \code{\link{parLapply}} for parallel lapply across
  separate processes.
}

\examples{
if (.Platform$OS.type != "windows") {
    ## default 2 cores, so 2 tasks (calls to FUN)
    ## compare with mclapply
    system.time(result <- pvec(1:10, function(v) {
        message("working")
        sqrt(v)
    }))
    result
}
}

\keyword{manip }
