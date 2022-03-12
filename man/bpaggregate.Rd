\name{bpaggregate}
\alias{bpaggregate}

\alias{bpaggregate,formula,BiocParallelParam-method}
\alias{bpaggregate,matrix,BiocParallelParam-method}
\alias{bpaggregate,data.frame,BiocParallelParam-method}
\alias{bpaggregate,ANY,missing-method}

\title{Apply a function on subsets of data frames}

\description{

    This is a parallel version of \code{\link[stats]{aggregate}}.

}

\usage{

\S4method{bpaggregate}{formula,BiocParallelParam}(x, data, FUN, ...,
    BPREDO=list(), BPPARAM=bpparam(), BPOPTIONS = bpoptions())

\S4method{bpaggregate}{data.frame,BiocParallelParam}(x, by, FUN, ...,
    simplify=TRUE, BPREDO=list(), BPPARAM=bpparam(), BPOPTIONS = bpoptions())

\S4method{bpaggregate}{matrix,BiocParallelParam}(x, by, FUN, ...,
    simplify=TRUE, BPREDO=list(),
    BPPARAM=bpparam(), BPOPTIONS = bpoptions()
)

\S4method{bpaggregate}{ANY,missing}(x, ..., BPREDO=list(),
    BPPARAM=bpparam(), BPOPTIONS = bpoptions()
)

}

\arguments{

  \item{x}{A \code{data.frame}, \code{matrix} or a formula.
  }
  \item{by}{A list of factors by which \code{x} is split;
    applicable when \code{x} is \code{data.frame} or \code{matrix}.
  }
  \item{data}{A \code{data.frame}; applicable when \code{x} is a
    \code{formula}.
  }
  \item{FUN}{Function to apply.
  }
  \item{...}{Additional arguments for \code{FUN}.
  }
  \item{simplify}{If set to \code{TRUE}, the return values of \code{FUN}
    will be simplified using \code{\link{simplify2array}}.
  }
  \item{BPPARAM}{An optional \code{\link{BiocParallelParam}} instance
    determining the parallel back-end to be used during evaluation.
  }
  \item{BPREDO}{A \code{list} of output from \code{bpaggregate} with one
    or more failed elements. When a list is given in \code{BPREDO},
    \code{bpok} is used to identify errors, tasks are rerun and inserted
    into the original results.
  }
  \item{BPOPTIONS}{
    Additional options to control the behavior of the parallel evaluation, see \code{\link{bpoptions}}.
  }
}

\details{

  \code{bpaggregate} is a generic with methods for \code{data.frame}
  \code{matrix} and \code{formula} objects. \code{x} is divided
  into subsets according to factors in \code{by}. Data chunks are
  sent to the workers, \code{FUN} is applied and results are returned
  as a \code{data.frame}.

  The function is similar in spirit to \code{\link[stats]{aggregate}}
  from the stats package but \code{\link[stats]{aggregate}} is not
  explicitly called. The \code{bpaggregate} \code{formula} method
  reformulates the call and dispatches to the \code{data.frame} method
  which in turn distributes data chunks to workers with \code{bplapply}.

}

\value{

  See \code{\link[stats]{aggregate}}.

}

\author{
  Martin Morgan \url{mailto:mtmorgan@fhcrc.org}.
}

\examples{

if (interactive() && require(Rsamtools) && require(GenomicAlignments)) {

  fl <- system.file("extdata", "ex1.bam", package="Rsamtools")
  param <- ScanBamParam(what = c("flag", "mapq"))
  gal <- readGAlignments(fl, param=param)

  ## Report the mean map quality by range cutoff:
  cutoff <- rep(0, length(gal))
  cutoff[start(gal) > 1000 & start(gal) < 1500] <- 1
  cutoff[start(gal) > 1500] <- 2
  bpaggregate(as.data.frame(mcols(gal)$mapq), list(cutoff = cutoff), mean)

}
}
