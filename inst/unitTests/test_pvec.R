test_pvec <- function() {
  ## Ensure that parallelism is actually tested
  options(cores=2, mc.cores=2)

  v <- setNames(1:12, letters[1:12])
  res1 <- sqrt(v)
  res2 <- parallel::pvec(v, sqrt)
  res3 <- BiocParallel::pvec(v, sqrt)
  checkIdentical(res1, res3)
  checkIdentical(res2, res3)

  ## Check on 1-element vectors
  checkIdentical(sqrt(5), parallel::pvec(5, sqrt))

  ## Check when forcing a single chunk
  checkIdentical(sqrt(v), parallel::pvec(v, sqrt, mc.num.chunks=1))

  ## Check when forcing more chunks than cores
  checkIdentical(sqrt(v), parallel::pvec(v, sqrt, mc.num.chunks=6))

  ## Check when forcing more chunks than elements in v
  checkIdentical(sqrt(v), parallel::pvec(v, sqrt, mc.num.chunks=20))
}

## test_pvec_bioc <- function() {
##   gr <- GenomicRanges::GRanges(
##     seqnames=sample(sprintf("chr%s", 1:26), size=1000, replace=TRUE),
##     ranges=IRanges::IRanges(start=runif(n=1000, min=1, max=1e6),
##       width=runif(n=1000, min=1, max=1000)),
##     strand=sample(c("+", "-"), size=1000, replace=TRUE))
##   ## Strand flip operation, returns a new GRanges object
##   res1 <- update(gr, strand=ifelse(strand(gr) == "+", "-", "+"))
##   res2 <- parallel::pvec(gr, function(x) update(x, strand=ifelse(strand(x) == "+", "-", "+")))
##   res3 <- BiocParallel::pvec(gr, function(x) update(x, strand=ifelse(strand(x) == "+", "-", "+")))
##   checkIdentical(res1, res3)
##   checkIdentical(res2, res3)
## }
