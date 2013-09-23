setGeneric("bpaggregate", function(x, ..., BPPARAM) standardGeneric("bpaggregate"))

setMethod("bpaggregate", c("ANY", "missing"),
    function(x, ..., BPPARAM) {
      bp = registered()[[1L]]
      bpaggregate(x, ..., BPPARAM=bp)
})

setMethod("bpaggregate", c("data.frame", "BiocParallelParam"),
  function(x, by, FUN, ..., pass.df = FALSE, BPPARAM) {
    FUN = match.fun(FUN)
    if (!is.data.frame(x))
      x = as.data.frame(x)
    if (!is.list(by))
      stop("'by' must be a list")
    by = lapply(by, factor)

    if (pass.df) {
      # FIXME the checks inside the wrapper are quite heuristic
      wrapper = function(.ind, .x, .AGGRFUN, ...) {
        res = .AGGRFUN(.x[.ind,, drop=FALSE], ...)
        if (is.list(res))
          res = unlist(res)
        if (!is.vector(res))
          stop("Result could not be converted to a vector")
        res
      }
    } else {
      wrapper = function(.ind, .x, .AGGRFUN, ...) {
        res = sapply(.x[.ind,, drop = FALSE], .AGGRFUN, ...)
        if (is.list(res))
          res = unlist(res)
        if (!is.vector(res))
          stop("Result could not be converted to a vector")
        res
      }
    }

    ind = split(seq_len(nrow(x)), by)
    grp = rep(seq_along(ind), sapply(ind, length))[match(seq_len(nrow(x)), unlist(ind))]
    res = bplapply(ind, wrapper, .x = x, .AGGRFUN = FUN, BPPARAM=BPPARAM)
    res = do.call(rbind, lapply(res, rbind))
    rownames(res) = NULL

    tab = as.data.frame(by)
    tab = tab[match(sort(unique(grp)), grp),, drop = FALSE]
    rownames(tab) = NULL
    tab = cbind(tab, res)
    if (!pass.df)
      names(tab) = c(names(by), names(x))
    tab
  }
)

setMethod("bpaggregate", c("formula", "BiocParallelParam"),
  function(x, data, FUN, ..., BPPARAM) {
    FUN = match.fun(FUN)

    lhs.missing = attr(terms(x, data = data), "response") == 0L
    if (lhs.missing) {
      rhs = all.vars(x[[2L]])
      lhs = names(data)
    } else {
      rhs = all.vars(x[[3L]])
      lhs = all.vars(x[[2L]])
      if (length(lhs) == 1L && lhs == ".")
        lhs = setdiff(names(data), rhs)
    }

    getVars = function(vars) {
      cmd = sprintf("list(%s)", paste(vars, collapse = ","))
      setNames(eval(parse(text = cmd), envir = data, enclos = environment(x)), vars)
    }

    bpaggregate(x = as.data.frame(getVars(lhs)), by = getVars(rhs), FUN, ...,
                pass.df=lhs.missing, BPPARAM=BPPARAM)
  }
)


function (x, by, FUN, ..., simplify = TRUE) 
{
    if (!is.data.frame(x)) 
        x <- as.data.frame(x)
    FUN <- match.fun(FUN)
    if (NROW(x) == 0L) 
        stop("no rows to aggregate")
    if (NCOL(x) == 0L) {
        x <- data.frame(x = rep(1, NROW(x)))
        return(aggregate.data.frame(x, by, function(x) 0L)[seq_along(by)])
    }
    if (!is.list(by)) 
        stop("'by' must be a list")
    if (is.null(names(by)) && length(by)) 
        names(by) <- paste("Group", seq_along(by), sep = ".")
    else {
        nam <- names(by)
        ind <- which(!nzchar(nam))
        names(by)[ind] <- paste("Group", ind, sep = ".")
    }
    nrx <- NROW(x)
    grp <- double(nrx)
    for (ind in rev(by)) {
        if (length(ind) != nrx) 
            stop("arguments must have same length")
        ind <- as.factor(ind)
        grp <- grp * nlevels(ind) + (as.integer(ind) - 1L)
    }
    y <- as.data.frame(by, stringsAsFactors = FALSE)
    y <- y[match(sort(unique(grp)), grp, 0L), , drop = FALSE]
    nry <- NROW(y)
    z <- lapply(x, function(e) {
        ans <- lapply(X = split(e, grp), FUN = FUN, ...)
        if (simplify && length(len <- unique(sapply(ans, length))) == 
            1L) {
            if (len == 1L) {
                cl <- lapply(ans, oldClass)
                cl1 <- cl[[1L]]
                ans <- unlist(ans, recursive = FALSE)
                if (!is.null(cl1) && all(sapply(cl, function(x) identical(x, 
                  cl1)))) 
                  class(ans) <- cl1
            }
            else if (len > 1L) 
                ans <- matrix(unlist(ans, recursive = FALSE), 
                  nrow = nry, ncol = len, byrow = TRUE, dimnames = {
                    if (!is.null(nms <- names(ans[[1L]]))) 
                      list(NULL, nms)
                    else NULL
                  })
        }
        ans
    })
    len <- length(y)
    for (i in seq_along(z)) y[[len + i]] <- z[[i]]
    names(y) <- c(names(by), names(x))
    row.names(y) <- NULL
    y
}
<bytecode: 0x31ca050>
<environment: namespace:stats>
