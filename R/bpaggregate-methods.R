# todo make these generics
bpaggregate.df = function(x, by, FUN, ..., pass.df = FALSE, BPPARAM) {
  FUN = match.fun(FUN)
  if (!is.data.frame(x))
    x = as.data.frame(x)
  if (!is.list(by))
    stop("'by' must be a list")
  by = lapply(by, factor)

  if (pass.df) {
    wrapper = function(.ind, .x, .FUN, ...) {
      as.list(.FUN(.x[.ind,, drop=FALSE], ...))
    }
  } else {
    wrapper = function(.ind, .x, .FUN, ...) {
      sapply(.x[.ind,, drop = FALSE], .FUN, ..., simplify=FALSE)
    }
  }

  ind = split(seq_len(nrow(x)), by)
  grp = rep(seq_along(ind), sapply(ind, length))[match(seq_len(nrow(x)), unlist(ind))]
  res = bplapply(ind, wrapper, .x = x, .FUN = FUN, BPPARAM=BPPARAM)
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

bpaggregate.formula = function(formula, data, FUN, ...) {
  FUN = match.fun(FUN)
  if (missing(formula) || !inherits(formula, "formula"))
    stop("'formula' missing or incorrect")

  lhs.missing = attr(terms(formula, data = data), "response") == 0L
  if (lhs.missing) {
    rhs = all.vars(formula[[2L]])
    lhs = names(data)
  } else {
    rhs = all.vars(formula[[3L]])
    lhs = all.vars(formula[[2L]])
    if (length(lhs) == 1L && lhs == ".")
      lhs = setdiff(names(data), rhs)
  }

  getVars = function(x) {
    cmd = sprintf("list(%s)", paste(x, collapse = ","))
    setNames(eval(parse(text = cmd), envir = data, enclos = environment(formula)), x)
  }

  bpaggregate(x = as.data.frame(getVars(lhs)), by = getVars(rhs), FUN, ..., pass.df=lhs.missing)
}

# TODO use this for testing
# data1 = iris
# data2 = cbind(iris, foo = sample(letters[1:2], nrow(iris), replace=TRUE))
# ext = sample(c("x", "y"), nrow(iris), replace=TRUE)
#
# aggr.formula(Sepal.Width ~ Species, data1, mean)
# aggr.formula(~ Species, data1, length)
# aggr.formula(Sepal.Width + Sepal.Length ~ Species, data1, mean)
# aggr.formula(Sepal.Width + Sepal.Length ~ Species + foo, data2, mean)
# aggr.formula(. ~ Species, data1, mean)
# aggr.formula(Sepal.Width ~ ext, data1, mean)
