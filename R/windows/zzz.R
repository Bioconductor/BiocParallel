mclapply <- parallel::mclapply

pvec <-
    function(..., AGGREGATE, mc.preschedule, num.chunks, chunk.size)
{
    parallel::pvec(...)
}
