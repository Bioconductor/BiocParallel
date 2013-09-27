mclapply <- parallel::mclapply

isChild <- function() {
    FALSE
}

pvec <-
    function(..., AGGREGATE, mc.preschedule, num.chunks, chunk.size)
{
    parallel::pvec(...)
}

setLoadActions(.registerDefaultParams = function(nmspc) {
    register(getOption("SnowParam", SnowParam(workers=detectCores())))
    register(getOption("BatchJobsParam", BatchJobsParam()))
    register(getOption("SerialParam", SerialParam()))
})
