test_bpaggregate = function() {
  data = cbind(iris, nonsense = sample(letters[1:3], nrow(iris), replace=TRUE))
  localvar = rnorm(nrow(iris))

  bp = SerialParam()
  register(bp)

  f = Sepal.Width ~ Species
  checkEquals(bpaggregate(f, data, mean),
              aggregate(f, data, mean))

  ### ... what happens here???
  # f = as.formula(Sepal.Width + Sepal.Length ~ Species)
  # x = bpaggregate(f, data, mean)
  # y = aggregate(f, data, mean)

  # checkEquals(x, y)
}
