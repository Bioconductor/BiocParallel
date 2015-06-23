## compatibility; used in mclapply, pvec
## lazy, to avoid caching stale versions

children <- function(...) parallel:::children(...)

closeStdout <- function(...) parallel:::closeStdout(...)

isChild <- function(...) parallel:::isChild(...)

mc.advance.stream <- function(...) parallel:::mc.advance.stream(...)

mc.set.stream <- function(...) parallel:::mc.set.stream(...)

mcexit <- function(...) parallel:::mcexit(...)

mcfork <- function(...) parallel:::mcfork(...)

mckill <- function(...) parallel:::mckill(...)

mc.reset.stream <- function(...) parallel::mc.reset.stream(...)

mcparallel <- function(...) parallel::mcparallel(...)

mccollect <- function(...) parallel::mccollect(...)

processID <-function(...) parallel:::processID(...)

readChild <- function(...) parallel:::readChild(...)

selectChildren <- function(...) parallel:::selectChildren(...)

sendMaster <- function(...) parallel:::sendMaster(...)

setLoadActions(.registerDefaultParams = function(nmspc) {
    tryCatch({
        ## these fail under complex conditions, e.g., loading a data
        ## set with a class defined in a package that imports
        ## BiocParallel
        register(getOption("MulticoreParam", MulticoreParam()), TRUE)
        register(getOption("SnowParam", SnowParam()), FALSE)
        register(getOption("SerialParam", SerialParam()), FALSE)
    }, error=function(err) {
        message("'BiocParallel' did not register default ",
                "BiocParallelParams:\n  ", conditionMessage(err))
        NULL
    })
})
