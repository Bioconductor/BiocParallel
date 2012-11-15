pvectorize <- function(FUN, vectorized.arg=1) {
  fargs <- names(as.list(args(FUN)))
  fargs <- fargs[-length(fargs)]
  if (is.numeric(vectorized.arg)) {
    vectorized.arg <- fargs[[vectorized.arg]]
  } else {
    vectorized.arg <- as.character(vectorized.arg)
  }
  stopifnot(vectorized.arg %in% fargs)
  rm(fargs)

  ## The wrapper ensures that the vectorized argument is called with
  ## the proper name.
  wrapper <- function(v, ...) {
    args <- list(...)
    args[[vectorized.arg]] <- v
    do.call(FUN, args)
  }

  ## Do some magic to support the same argument name for the
  ## vectorized arg as the original function.
  vecfun <- function(THEFIRSTARGUMENT, ...) {
    pvec(v=get(vectorized.arg), FUN=wrapper, ...)
  }
  names(formals(vecfun))[[1]] <- vectorized.arg
  vecfun
}
