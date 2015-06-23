mclapply <- parallel::mclapply

.bpiterate <- function(ITER, FUN, ...)
    bpiterate(ITER, FUN, ..., BPPARAM=SerialParam())

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
        register(getOption("SnowParam", SnowParam()), TRUE)
        register(getOption("SerialParam", SerialParam()), FALSE)
    }, error=function(err) {
        message("'BiocParallel' did not register default BiocParallelParams")
        NULL
    })
})
