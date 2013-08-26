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

