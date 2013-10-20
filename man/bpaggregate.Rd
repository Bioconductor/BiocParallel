\name{bpaggregate}
\alias{bpaggregate}

\alias{bpaggregate,formula,BiocParallelParam-method}
\alias{bpaggregate,data.frame,BiocParallelParam-method}
\alias{bpaggregate,ANY,missing-method}

\title{Apply a function on subsets of data frames}

\description{

    This is a parallel version of \code{\link[stats]{aggregate}}.

}

\usage{

\S4method{bpaggregate}{formula,BiocParallelParam}(x, data, FUN, ..., BPPARAM)

\S4method{bpaggregate}{data.frame,BiocParallelParam}(x, by, FUN, ..., simplify=TRUE, BPPARAM)

\S4method{bpaggregate}{ANY,missing}(x, ..., BPPARAM)

}

\arguments{
  
  \item{x}{A data frame or a formula.}
  \item{by}{A list of factors. Data frame \code{x} will be split
      accoring to the factors.}
  \item{data}{A data frame.}
  \item{FUN}{Function to apply.}
  \item{...}{Additional arguments for \code{FUN}.}
  \item{simplify}{If set to \code{TRUE}, the return values of \code{FUN}
      will be simplified using \code{\link[base]{simplify2array}}.}
  \item{BPPARAM}{An optional \code{\link{BiocParallelParam}} instance
      determining the parallel back-end to be used during evaluation.}
}


\value{

  See \code{\link[stats]{aggregate}}.

}

\author{
  Martin Morgan \url{mailto:mtmorgan@fhcrc.org}.
}
