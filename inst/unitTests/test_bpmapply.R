test_bpmapply_Params <- function() {
  # FIXME we need the windows workaround
  params <- list(serial=SerialParam(),
                 snow0=SnowParam(2, "FORK"),
                 snow1=SnowParam(2, "PSOCK"),
                 batchjobs=BatchJobsParam(),
                 multi=MulticoreParam(),
                 dopar=DoparParam())

  x = 1:10
  y = rev(x)
  f = function(x, y) x + y
  expected = x + y
  for (param in params) {
    checkIdentical(expected, bpmapply(f, x, y, BPPARAM=param))
  }

  # test names and simplify
  x = setNames(1:5, letters[1:5])
  for (param in params) {
    for (SIMPLIFY in c(FALSE, TRUE)) {
      for (USE.NAMES in c(FALSE, TRUE)) {
        expected = mapply(identity, x, USE.NAMES=USE.NAMES, SIMPLIFY=SIMPLIFY)
        result = bpmapply(identity, x, USE.NAMES=USE.NAMES, SIMPLIFY=SIMPLIFY, BPPARAM=param)
        checkIdentical(expected, result)
      }
    }
  }


  # test MoreArgs
  x = setNames(1:5, letters[1:5])
  f = function(x, m) { x + m }
  expected = mapply(f, x, MoreArgs = list(m = 1))
  for (param in params) {
    checkIdentical(expected, bpmapply(f, x, MoreArgs = list(m = 1), BPPARAM=param))
  }

  TRUE
}
