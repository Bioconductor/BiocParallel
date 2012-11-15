test_pvectorize <- function() {

  psqrt <- pvectorize(sqrt)
  checkIdentical(psqrt(1:1000), sqrt(1:1000))

  # Contrived example with a vectorized second argument and scalar
  # first argument
  myfun <- function(offset, v) v + offset[[1]]
  res1 <- myfun(2, 1:50)
  pmyfun <- pvectorize(myfun, vectorized.arg="v")
  # Argument "v" comes first now
  res2 <- pmyfun(1:50, 2)
  # Avoid problems with argument order by using named args
  res3 <- pmyfun(offset=2, v=1:50)

  checkIdentical(res1, res2)
  checkIdentical(res1, res3)
}
