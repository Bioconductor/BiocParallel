test_bpmapply_Params <- function() {
  # FIXME multicore missing / always crashes using gentoo :(
  # FIXME we need the windows workaround
  params <- list(serial=SerialParam(),
                 snow0=SnowParam(2, "FORK"),
                 snow1=SnowParam(2, "PSOCK"),
                 batchjobs=BatchJobsParam(),
                 dopar=DoparParam())

  x = 1:10
  y = rev(x)
  f = function(x, y) x + y
  expected = x + y
  for (param in params) {
    checkIdentical(expected, bpmapply(f, x, y, BPPARAM=param))
  }

  # test names
  x = setNames(1:5, letters[1:5])
  expected = mapply(identity, x)
  for (param in params) {
    checkIdentical(expected, bpmapply(identity, x, BPPARAM=param))
  }

  # test simplify
  x = setNames(1:5, letters[1:5])
  expected = mapply(identity, x, SIMPLIFY=FALSE)
  for (param in params) {
    checkIdentical(expected, bpmapply(identity, x, SIMPLIFY=FALSE, BPPARAM=param))
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
