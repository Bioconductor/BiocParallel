mclapply <- parallel::mclapply

pvec <- function(..., mc.preschedule, num.chunks, chunk.size)
  parallel::pvec(...)
