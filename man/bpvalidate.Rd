\name{bpvalidate}
\alias{bpvalidate}

\title{Validate functions to be used in parallel evaluation}

\description{
  Test that a function is `self-contained` and can be run in a distributed 
  memory environment.
}

\usage{
bpvalidate(fun)
}

\arguments{
  \item{fun}{
    The function to be checked.}
}

\details{
  \code{bpvalidate} tests whether a function is `self-contained` such that
  it can be run in a distributed memory environment (e.g., clusters, or Windows 
  machines). \code{bpvalidate} checks that all symbols are defined in the
  search path or NAMESPACE of \code{fun} and all required libraries are loaded
  in the body of \code{fun}. The output is a list of undefined symbols and
  required packages, if any.

  \code{bpvalidate} can be used to check functions passed to the bp* family
  of functions in \code{BiocParallel} or other packages that support parallel
  evaluation on clusters such as \code{snow}, \code{BatchJobs}, \code{Rmpi}, 
  etc.

  When \code{bpvalidate} is used to test a function called from within a 
  package, that package should be loaded before calling \code{bpvalidate(fun)}. 
  This loads the necessary libraries and simulates the environment present
  when parallel execution occurs.
}

\value{
  A \code{list} of length 2 with named elements `RequiredPackages`
  and `UnknownSymbols`.
  \itemize{
    \item{RequiredPackages}{
      A vector of package names required for execution of \code{fun}. Packages
      that appear in this list should be loaded in the body of \code{fun}
      with library() or require(). 
    }
    \item{UnknownSymbols}{
      A vector of undefined symbols. These may be function or argument names.
    }
  }
}

\author{
  Martin Morgan \url{mailto:mtmorgan@fhcrc.org} and
  Valerie Obenchain \url{mailto:vobencha@fhcrc.org}.
}

\examples{

library(Rsamtools)
library(RNAseqData.HNRNPC.bam.chr14)
files <- RNAseqData.HNRNPC.bam.chr14_BAMFILES
param <- ScanBamParam(flag=scanBamFlag(isMinusStrand=FALSE))

## Forgetting to load libraries or explicitly pass arguments are 
## common errors when writing functions for cluster use. The examples below 
## show how bpvalidate() can help identify these problems.

## ---------------------------------------------------------------------
## A. Loading libraries:
## ---------------------------------------------------------------------

## This function counts records on the "+" strand in BAM files with
## countBam() from Rsamtools. 
fun1 <- function(fl, ...) countBam(fl)

## bpvalidate() suggests that Rsamtools be loaded in the body of 'fun': 
res1 <- bpvalidate(fun1)
res1$RequiredPackages
res1$UnknownSymbols

## When Rsamtools is explicitly loaded we see it still appears in 
## 'RequiredPackages'. This list serves as a check/reminder of what
## packages should be loaded and does not indicate the function is invalid.
fun2 <- function(fl, ...) {
    library(Rsamtools)
    countBam(fl)
}
bpvalidate(fun2)$RequiredPackages

## ---------------------------------------------------------------------
## B. Passing arguments:
## ---------------------------------------------------------------------

## Here 'param' is used in the body but not specified as an argument.
fun3 <- function(fl, ...) {
  library(Rsamtools)
  countBam(fl, param=param)
}

## bpvalidate() flags 'param' as unknown:
bpvalidate(fun3)$UnknownSymbols

## If we add 'param' as a formal argument it is not flagged:
fun4 <- function(fl, ..., param) {
  library(Rsamtools)
  countBam(fl, param=param)
}
bpvalidate(fun4)$UnknownSymbols

## Note that 'param' must be passed to the bp* function explicitly.
## This call will fail:
\dontrun{bplapply(files, fun4, BPPARAM=SnowParam(2))}

## This call is correct: 
\dontrun{bplapply(files, fun4, param=param, BPPARAM=SnowParam(2))}
}

\keyword{manip}
