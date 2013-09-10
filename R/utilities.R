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

.renameSimplify = function(results, dots, USE.NAMES=FALSE, SIMPLIFY=FALSE) {
    if (USE.NAMES) {
      if (length(dots)) {
        if (is.null(names(dots[[1L]]))) {
            if(is.character(dots[[1L]]))
              names(results) = dots[[1L]]
        } else {
          names(results) = names(dots[[1L]])
        }
      }
    }

    if (SIMPLIFY) {
      results = simplify2array(results)
    }

    return(results)
}

.getDotsForMapply = function(...) {
  ddd = list(...)
  if (length(ddd)) {
    len = vapply(ddd, length, integer(1L))
    if (!all(len == len[1L])) {
      max.len = max(len)
      if (any(max.len %% len))
        warning("longer argument not a multiple of length of vector")
      ddd = lapply(ddd, rep_len, length.out = max.len)
    }
  }

  return(ddd)
}
