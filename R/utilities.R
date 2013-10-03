.splitIndices <- function (nx, ncl)
{
    ## derived from parallel
    i <- seq_len(nx)
    if (ncl <= 1L || nx <= 1L)          # allow nx, nc == 0
        i
    else {
        fuzz <- min((nx - 1L)/1000, 0.4 * nx/ncl)
        breaks <- seq(1 - fuzz, nx + fuzz, length = ncl + 1L)
        structure(split(i, cut(i, breaks)), names = NULL)
    }
}

.rename <-
    function(results, dots, USE.NAMES=FALSE)
{
    if (USE.NAMES && length(dots)) {
        if (is.null(names(dots[[1L]]))) {
            if(is.character(dots[[1L]]))
                names(results) <- dots[[1L]]
        } else {
            names(results) <- names(dots[[1L]])
        }
    }
    results
}

.simplify <-
    function(results, SIMPLIFY=FALSE)
{
    if (SIMPLIFY && length(results))
        return(simplify2array(results))
    results
}


.getDotsForMapply <-
    function(...)
{
    ddd <- list(...)
    if (!length(ddd))
      return(list())
    len <- vapply(ddd, length, integer(1L))
    if (!all(len == len[1L])) {
        max.len <- max(len)
        if (max.len && any(len == 0L))
            stop("zero-length and non-zero length inputs cannot be mixed")
        if (any(max.len %% len))
            warning("Longer argument not a multiple of length of vector")
        ddd <- lapply(ddd, rep_len, length.out=max.len)
    }

  return(ddd)
}

## The following 3 functions are useful for implementing multi-arg
## recycling (e.g. for bpmapply) without expanding all args to the
## length of the largest one (which might take a lot of memory for no
## reason).

## Returns the appropriate length for recycled vectors, and also warns
## about incomplte recycling
recycleLength <- function(..., ARGS, warn=TRUE) {
    if (missing(ARGS))
        ARGS <- list(...)
    if (length(ARGS) == 1)
        return(length(ARGS[[1]]))
    arglengths <- sapply(ARGS, length)
    maxlen <- max(arglengths)
    if (warn) {
        for (i in seq_along(arglengths)) {
            arglen <- arglengths[i]
            argname <- names(arglengths)[i]
            if (is.null(argname) || is.na(argname)) argname <- as.character(i)
            if (maxlen %% arglen > 0) {
                warning("length of result is not a multiple of vector length (arg ", argname, ")")
            }
        }
    }
    maxlen
}

## Like "[" with recycling of the vector
indexRecycled <- function(v, i) {
    v[1+(i-1)%%length(v)]
}

## Like "[[" with recycling of the vector
indexRecycledSingle <- function(v, i) {
    v[[1+(i-1)%%length(v)]]
}

## This works like the real formals function, but it also works on
## most primitives where formals does not.
.formals <- function(fun) as.pairlist(head(as.list(args(fun)), -1))
