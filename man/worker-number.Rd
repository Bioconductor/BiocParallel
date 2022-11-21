\name{workers}

% Environment variables
\alias{BIOCPARALLEL_WORKER_NUMBER}
\alias{BIOCPARALLEL_WORKER_MAX}
\alias{R_PARALLELLY_AVAILABLECORES_FALLBACK}

\title{Environment control of worker number}

\description{

  Environment variables, global options, and aspects of the computing
  environment controlling default and maximum worker number.

}

\details{

  By default, BiocParallel \code{Param} objects use almost all
  (\code{parallel::detectCores() - 2}) available cores as
  workers. Several variables can determine alternative default number of
  workers. Elements earlier in the description below override elements
  later in the description.

  \describe{

    \item{\code{_R_CHECK_LIMIT_CORES_}:}{Environment variable defined in
      base R, described in the 'R Internals' manual
      (\code{RShowDoc("R-ints")}). If defined and not equal to
      \code{"false"} or \code{"FALSE"}, default to 2 workers.}

    \item{\code{IS_BIOC_BUILD_MACHINE}:}{Environment variable used by
      the Bioconductor build system; when defined, default to 4
      workers.}

    \item{\code{getOption("mc.cores")}:}{Global R option (initialized
      from the environment variable \code{MC_CORES}) with non-negative
      integer number of workers, also recognized by the base R
      'parallel' package.}

    \item{\code{BIOCPARALLEL_WORKER_MAX:}}{Environment variable,
      non-negative integer number of workers. Use this to set both the
      default and maximum worker number to a single value.}

    \item{\code{BIOCPARALLEL_WORKER_NUMBER}:}{Environment variable,
      non-negative integer number of workers. Use this to set a default
      worker number without specifying \code{BIOCPARALLEL_WORKER_MAX},
      or to set a default number of workers less than the maximum
      number.}

    \item{\code{R_PARALLELLY_AVAILABLECORES_FALLBACK}:}{Environment
      variable, non-negative integer number of workers, also recognized by
      the 'parallelly' family of packages.}

  }

  A subset of environment variables and other aspects of the computing
  environment also \emph{enforce} limits on worker number. Usually, a
  request for more than the maximum number of workers results in a
  warning message and creation of a 'Param' object with the maximum
  rather than requested number of workers.

  \describe{

    \item{\code{_R_CHECK_LIMIT_CORES_}:}{Environment variable defined in
      base R. \code{"warn"} limits the number of workers to 2, with a
      warning; \code{"false"}, or \code{"FALSE"} does not limit worker
      number; any other value generates an error.}

    \item{\code{IS_BIOC_BUILD_MACHINE}:}{Environment variable used by
      the Bioconductor build system. When set, limit the number of
      workers to 4.}

    \item{\code{BIOCPARALLEL_WORKER_MAX:}}{Environment variable,
      non-negative integer.}

    \item{Number of available connections:}{R has an internal limit
      (126) on the number of connections open at any time. 'SnowParam()'
      and 'MulticoreParam()' use 1 connection per worker, and so are
      limited by the number of available connections.}

  }

}

\examples{
## set up example
original_worker_max <- Sys.getenv("BIOCPARALLEL_WORKER_MAX", NA_integer_)
original_worker_n <- Sys.getenv("BIOCPARALLEL_WORKER_NUMBER", NA_integer_)
Sys.setenv(BIOCPARALLEL_WORKER_MAX = 4)
Sys.setenv(BIOCPARALLEL_WORKER_NUMBER = 2)

bpnworkers(SnowParam())  # 2
bpnworkers(SnowParam(4)) # OK
bpnworkers(SnowParam(5)) # warning; set to 4

## clean up
Sys.unsetenv("BIOCPARALLEL_WORKER_MAX")
if (!is.na(original_worker_max))
    Sys.setenv(BIOCPARALLEL_WORKER_MAX = original_worker_max)
Sys.unsetenv("BIOCPARALLEL_WORKER_NUMBER")
if (!is.na(original_worker_n))
    Sys.setenv(BIOCPARALLEL_WORKER_NUMBER = original_worker_n)
}
