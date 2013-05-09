mclapply <- parallel::mclapply

isChild <- function() {
    FALSE
}

pvec <-
    function(..., AGGREGATE, mc.preschedule, num.chunks, chunk.size)
{
    parallel::pvec(...)
}

.onLoad <-
    function(libname, pkgname)
{
    register(getOption("SnowParam", SnowParam(workers=detectCores())))
}
