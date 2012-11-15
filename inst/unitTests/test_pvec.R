test_pvec <- function() {
  v <- 1:1000
  res1 <- sqrt(v)
  res2 <- parallel::pvec(v, sqrt)
  res3 <- BiocParallel::pvec(v, sqrt)
  checkIdentical(res1, res3)
  checkIdentical(res2, res3)
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
