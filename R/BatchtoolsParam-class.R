### ================================================================
### BatchtoolsParam objects
### ----------------------------------------------------------------

batchtoolsWorkers <-
    function()
{
    .snowCores(multicore=.Platform$OS.type != "windows")
}


## Return batchtools.Multicore_conf.R as the default path.
batchtoolsConf <-
    function(path="inst/batchtools.Multicore_conf.R")
{
    conf <- path
    conf
}


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

setOldClass("Registry")

.BatchtoolsParam <- setRefClass("BatchtoolsParam",
     contains="BiocParallelParam",
     field = list(
         reg = "Registry",
         conf.file = "character"
     ),
     methods = list(
         ## Initialize
         initialize = function(...,
             conf.file="character",
             workers=NA_integer_)
         {
             ## Get workers
             x_workers <- if (is.na(workers)) {
                              getNumberCPUs <- function(reg) {
                                  x <- environment(reg$cluster.functions$submitJob)$p
                                  x[["ncpus"]]
                              }       
                          }
             
             ## Get config
             conf <- batchtoolsConf()
         },
                               
         ## Show 
         show = function() {
             callSuper()
             cat(" registry: ", .self$reg,
                 "\n conf.file: ", .self$conf.file,
                 sep = "")
         }
     )
)

BatchtoolsParam <-
    function(workers=batchtoolsWorkers(),
             conf.file=batchtoolsConf(),
             ...)
{


    x <- .BatchtoolsParam(
        workers = workers,
        reg = reg,
        conf.file = conf.file)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - control
###

setMethod("bpisup", "BatchtoolsParam",
          function(x)
          {
              TRUE
          })

setMethod("bpstart", "BatchtoolsParam",
          function(x)
{
    reg <- batchtools::makeRegistry(file.dir=tempfile("registry_", "."),
                                    conf.file = conf.file)
    ## TODO: set the registry to BatchtoolsParam
    setRegistry(x) <- reg
})


setMethod("bpstop", "BatchtoolsParam",
          function(x)
{
    ## TODO: getRegistry
    reg <- getRegistry(x)
    batchtools::removeRegistry(reg=reg)
})


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - evaluation
###

setMethod("bplapply", c("ANY", "BatchtoolsParam"),
          function(X, FUN, ..., BPPARAM=bpparam())
{
    ## TODO: getRegistery(X) / explore getDefaultRegistry()
    reg <- getRegistry(X)

    if (missing(reg)) {
        reg = batchtools::getDefaultRegistry()
    }
    jobIds <- batchtools::batchMap(fun=FUN,
                                   n=workers,
                                   reg=reg)
    batchtools::submitJobs(jobIds, reg=reg)
    batchtools::waitForJobs(reg=reg)
    ## Explore reduceResultsList and reduceResults
    result <- lapply(seq_len(workers), loadResult, reg=reg)

    ## Make invisible call to clear registry
    invisible(batchtools::clearRegistry(reg=reg))
})
