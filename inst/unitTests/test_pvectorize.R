test_pvectorize <- function() {

  psqrt <- pvectorize(sqrt)
  checkIdentical(psqrt(1:1000), sqrt(1:1000))

  # Contrived example with a vectorized second argument and scalar
  # first argument
  myfun <- function(offset, v) v + offset[[1]]
  res1 <- myfun(2, 1:50)
  pmyfun <- pvectorize(myfun, vectorized.arg="v")
  # Argument "v" comes first now
  res2 <- pmyfun(2, 1:50)
  # Make sure named out-of-order args work
  res3 <- pmyfun(v=1:50, offset=2)
  # Make sure mc.* args are accepted
  res4 <- pmyfun(2, 1:50, mc.preschedule=FALSE, mc.chunk.size=10)

  checkIdentical(res1, res2)
  checkIdentical(res1, res3)
  checkIdentical(res1, res4)

  # Function with multiple vectorized arguments and one scalar argument
  myfun2 <- function(x, y, denom) (x+y)/denom[[1]]
  pmyfun2 <- pvectorize(myfun2, vectorize.args=c("x", "y"))
  ## Intentionally pass a length-2 vector for the scalar argument to
  ## ensure that only the first element is used.
  checkIdentical(myfun2(1:50, 1:100, c(5, 6)), pmyfun2(1:50, 1:100, c(5, 6)))
}
