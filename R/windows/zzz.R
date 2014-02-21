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
    tryCatch({
        ## these fail under complex conditions, e.g., loading a data
        ## set with a class defined in a package that imports
        ## BiocParallel
        register(getOption("SerialParam", SerialParam()))
        register(getOption("BatchJobsParam", BatchJobsParam()))
        register(getOption("SnowParam", SnowParam(workers=detectCores())))
    }, error=function(err) {
        message("'BiocParallel' did not register default BiocParallelParams")
        NULL
    })
})
