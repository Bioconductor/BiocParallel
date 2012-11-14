\name{mclapply}
\alias{mclapply}
\alias{pvec}

\title{Serial versions of multicore functions.}
\description{
  These are simple serial versions of \code{mclapply} and \code{pvec},
  directly exposing functions implemented in the \code{parallel} package.
}
\usage{
mclapply(X, FUN, ..., mc.preschedule = TRUE, mc.set.seed = TRUE,
         mc.silent = FALSE, mc.cores = 1L,
         mc.cleanup = TRUE, mc.allow.recursive = TRUE)

pvec(v, FUN, ..., mc.set.seed = TRUE, mc.silent = FALSE,
     mc.cores = 1L, mc.cleanup = TRUE)
}
\arguments{

  \item{X, v, FUN, f, ..., MoreArgs, SIMPLIFY, USE.NAMES,
    mc.preschedule, mc.set.seed, mc.silent, mc.cleanup,
    mc.allow.recursive, mc.cores}{
    These arguments are as documented in
    \code{\link[parallel]{mclapply}} in the \code{parallel} package.
  }
}


\value{See \code{\link[parallel]{pvec}}.}

\keyword{interface}
