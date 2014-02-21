## compatibility; used in mclapply, pvec

children <- parallel:::children

closeStdout <- parallel:::closeStdout

isChild <- parallel:::isChild

mc.advance.stream <- parallel:::mc.advance.stream

mc.set.stream <- parallel:::mc.set.stream

mcexit <- parallel:::mcexit

mcfork <- parallel:::mcfork

mckill <- parallel:::mckill

processID <-parallel:::processID

readChild <- parallel:::readChild

selectChildren <- parallel:::selectChildren

sendMaster <- parallel:::sendMaster

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
