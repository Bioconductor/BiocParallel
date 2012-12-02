\name{BiocParallel-NEWS}
\title{BiocParallel News}

\section{CHANGES IN VERSION 0.99.0}{

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

    }

  }

%%  \subsection{SIGNIFICANT USER-VISIBLE CHANGES}{}

%%  \subsection{BUG FIXES}{}

}
