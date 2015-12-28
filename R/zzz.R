setLoadActions(.registerDefaultParams = function(nmspc) {
    ## these fail under complex conditions, e.g., loading a data
    ## set with a class defined in a package that imports
    ## BiocParallel
    multicore <- (parallel::detectCores() - 2L) > 1L
    tryCatch({
        if ((.Platform$OS.type == "windows") && multicore) {
            register(getOption("SnowParam", SnowParam()), TRUE)
            register(getOption("SerialParam", SerialParam()), FALSE)
        } else if (multicore) {
            ## linux / mac
            register(getOption("MulticoreParam", MulticoreParam()), TRUE)
            register(getOption("SnowParam", SnowParam()), FALSE)
            register(getOption("SerialParam", SerialParam()), FALSE)
        } else {
            register(getOption("SerialParam", SerialParam()), TRUE)
        }
    }, error=function(err) {
        message("'BiocParallel' did not register default ",
                "BiocParallelParams:\n  ", conditionMessage(err))
        NULL
    })
})
