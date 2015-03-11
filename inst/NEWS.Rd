\name{BiocParallel-NEWS}
\title{BiocParallel News}

\section{CHANGES IN VERSION 0.5.5}{

  \subsection{NEW FEATURES}{

    \itemize{

      \item \code{multicoreWorkers()} determines the number of workers
      based on operating system (Windows: 1), user preference (via the
      global option \code{options(mc.cores=...)}), or system capability
      (\code{detectCores()}).

      \item \code{bpparam()} selects a default \code{BiocParallelParam},
      from global options or, if that fails, the most recently
      \code{registered()} back-end.

    }
  }

  \subsection{SIGNIFICANT USER-VISIBLE CHANGES}{

    \itemize{

      \item Rename argument controlling resumption on error as
      \code{BPRESUME}

      \item Default to parallel back-end (multicore on non-Windows; snow
      on Windows).

    }
  }

  \subsection{BUG FIXES}{

    \itemize{

      \item bpvec,ANY,MulticoreParam-method with fewer tasks than cores
      evaluates only the cores for which tasks are defined.

    }
  }
}



\section{CHANGES IN VERSION 0.5.2}{

  \subsection{NEW FEATURES}{

    \itemize{
      \item \code{mclapply()}, \code{pvec()} require only \code{length},
      \code{[}, and (for \code{mclapply}) \code{[[}.

      \item \code{pvectorize()} creates a parallel version of its
      vectorized function argument.

      \item \code{MulticoreParam}, \code{SnowParam}, \code{DoparParam}
      (foreach-derived), \code{SerialParam} to parameterize back-ends.

      \item \code{bplapply}, \code{bpvec} as parallel evaluation
      models.

      \item \code{bpstart}, \code{bpstop}, \code{bpisup} for back-end
      management.

      \item \code{bpvec} has a new argument \code{AGGREGATE}, a function
      to specify how results are to be combined.

      \item Support for BatchJobs back-end added, via GSOC Michel Lang.

    }

  }

  \subsection{SIGNIFICANT USER-VISIBLE CHANGES}{

    \itemize{

      \item \code{BPPARM} is now used as the argument name for passing
      BiocParallelParam instances to functions.

      \item \code{bplapply} and \code{bpvec} now only dispatch on
      \code{X} and \code{BPPARAM}.

    }
  }

%%  \subsection{BUG FIXES}{}

}
