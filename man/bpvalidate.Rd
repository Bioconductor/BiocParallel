\name{bpvalidate}
\alias{bpvalidate}
\alias{BPValidate-class}
\alias{show,BPValidate-method}

\title{Tools for developing functions for parallel execution in
       distributed memory}

\description{
  \code{bpvalidate} interrogates the function environment and search path
  to locate undefined symbols.
}

\usage{
bpvalidate(fun, signal = c("warning", "error", "silent"))
}

\arguments{

  \item{fun}{The function to be checked. \code{typeof(fun)} must return
    either \code{"closure"} or \code{"builtin"}.}

  \item{signal}{\code{character(1)} matching \code{"warning", "error",
    "silent"} or a function with signature \code{(..., call.)} to be
    invoked when reporting errors. Using \code{"silent"} suppresses
    output; \code{"warning"} and \code{"error"} emit warnings or errors
    when \code{fun} contains references to unknown variables or
    variables defined in the global environment (and hence not
    serialized to workers).}

}

\details{
  \code{bpvalidate} tests if a function can be run in a distributed memory
  environment (e.g., SOCK clusters, Windows machines). \code{bpvalidate} looks
  in the environment of \code{fun}, in the NAMESPACE exports of libraries
  loaded in \code{fun}, and along the search path to identify any symbols 
  outside the scope of \code{fun}.

  \code{bpvalidate} can be used to check functions passed to the bp*
  family of functions in \code{BiocParallel} or other packages that
  support parallel evaluation on clusters such as \code{snow},
  \code{Rmpi}, etc.

  \describe{
    \item{testing package functions}{
      The environment of a function defined inside a package is the 
      NAMESPACE of the package. It is important to test these functions
      as they will be called from within the package, with the appropriate
      environment. Specifically, do not copy/paste the function into
      the workspace; once this is done the GlobalEnv becomes the function
      environment.
      
      To test a package function, load the package then call the function by 
      name (myfun) or explicitly (mypkg:::myfun) if not exported.
    }
    \item{testing workspace functions}{
      The environment of a function defined in the workspace is the GlobalEnv.
      Because these functions do not have an associated package NAMESPACE,
      the functions and variables used in the body must be explicitly passed
      or defined. See examples.

      Defining functions in the workspace is often done during development or
      testing. If the function is later moved inside a package, it can be
      rewritten in a more lightweight form by taking advantage of imported
      symbols in the package NAMESPACE.
    }
  }
  NOTE: \code{bpvalidate} does not currently work on Generics.
}

\value{
  An object of class \code{BPValidate} summarizing symbols identified in
  the global environment or search path, or undefined in the
  enviornments the function was defined in. Details are only available
  via `show()`.
}

\author{
  Martin Morgan \url{mailto:mtmorgan.bioc@gmail.com} and
  Valerie Obenchain.
}

\examples{

## ---------------------------------------------------------------------
## Interactive use
## ---------------------------------------------------------------------

fun <- function()
    .__UNKNOWN_SYMBOL__
bpvalidate(fun, "silent")

## ---------------------------------------------------------------------
## Testing package functions
## ---------------------------------------------------------------------

\dontrun{
library(myPkg)

## Test exported functions by name or the double colon:
bpvalidate(myExportedFun)
bpvalidate(myPkg::myExportedFun)

## Non-exported functions are called with the triple colon:
bpvalidate(myPkg:::myInternalFun)
}

## ---------------------------------------------------------------------
## Testing workspace functions
## ---------------------------------------------------------------------

## Functions defined in the workspace have the .GlobalEnv as their
## environment. Often the symbols used inside the function body
## are not defined in .GlobalEnv and must be passed explicitly.

## Loading libraries:
## In 'fun1' countBam() is flagged as unknown:
fun1 <- function(fl, ...) 
    countBam(fl)
v <- bpvalidate(fun1)

## countBam() is not defined in .GlobalEnv and must be passed as
## an argument or made available by loading the library.
fun2 <- function(fl, ...) {
    Rsamtools::countBam(fl)
}
v <- bpvalidate(fun2)

## Passing arguments:
## 'param' is defined in the workspace but not passed to 'fun3'. 
## bpvalidate() flags 'param' as being found '.GlobalEnv' which means
## it is not defined in the function environment or inside the function.
library(Rsamtools)
param <- ScanBamParam(flag=scanBamFlag(isMinusStrand=FALSE))

fun3 <- function(fl, ...) {
  Rsamtools::countBam(fl, param=param)
}
v <- bpvalidate(fun3)

## 'param' is explicitly passed by adding it as a formal argument.
fun4 <- function(fl, ..., param) {
  Rsamtools::countBam(fl, param=param)
}
bpvalidate(fun4)

## The corresponding call to a bp* function includes 'param':
\dontrun{
bplapply(files, fun4, param=param, BPPARAM=SnowParam(2))
}

}

\keyword{manip}
