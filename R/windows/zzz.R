mclapply <- parallel::mclapply

.bpiterate <- function(ITER, FUN, ..., REDUCE)
{
    res <- list()
    while (!is.null(dat <- ITER()))
        res <- c(res, FUN(dat, ...))

    if (!missing(REDUCE))
        REDUCE(res, ...)
    else
        res
}

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
        register(getOption("MulticoreParam", MulticoreParam()))
    }, error=function(err) {
        message("'BiocParallel' did not register default BiocParallelParams")
        NULL
    })
})
