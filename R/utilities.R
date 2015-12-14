.paramFields <-
    function(generator)
{
    result <- generator$fields()
    result[setdiff(names(result), names(.BiocParallelParam$fields()))]
}

.splitIndices <- function (nx, tasks)
{
    ## derived from parallel
    i <- seq_len(nx)
    if (nx == 0L)
        list()
    else if (tasks <= 1L || nx == 1L)  # allow nx, nc == 0
        list(i)
    else {
        fuzz <- min((nx - 1L)/1000, 0.4 * nx / tasks)
        breaks <- seq(1 - fuzz, nx + fuzz, length = tasks + 1L)
        si <- structure(split(i, cut(i, breaks)), names = NULL)
        si[sapply(si, length) != 0]
    }
}

.splitX <- function(X, workers, tasks) 
{
    if (tasks == 0L)
        tasks <- workers
    idx <- .splitIndices(length(X), tasks)
    relist(X, idx)
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
