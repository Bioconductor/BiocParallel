setMethod(bpmapply, c("function", "missing"),
  function(FUN, ..., MoreArgs = NULL, SIMPLIFY = TRUE, USE.NAMES = TRUE, BPPARAM) {
    FUN <- match.fun(FUN)
    x <- registered()[[1]]
    bpmapply(FUN, ..., MoreArgs = MoreArgs, SIMPLIFY = SIMPLIFY,
             USE.NAMES = USE.NAMES, BPPARAM = x)
})
