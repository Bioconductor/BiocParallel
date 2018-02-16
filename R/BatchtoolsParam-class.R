### ================================================================
### BatchtoolsParam objects
### ----------------------------------------------------------------

batchtoolsWorkers <-
    function(cluster = c("socket", "multicore", "interactive"))
{
    switch(
        match.arg(cluster),
        interactive = 1L,
        .snowCores(multicore=.Platform$OS.type != "windows")
    )
}

batchtoolsCluster <-
    function(cluster = c("socket", "multicore", "interactive"))
{
    if (missing(cluster)) {
        if (.Platform$OS.type == "windows") {
            cluster <- "socket"
        } else {
            cluster <- "multicore"
        }
    } else {
        cluster <- match.arg(cluster)
    }
    cluster
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

setOldClass("Registry")

.BatchtoolsParam <- setRefClass(
    "BatchtoolsParam",
    contains="BiocParallelParam",
    field = list(
        cluster = "character"
    ),
    methods = list(
        show = function() {
            callSuper()
            cat("  cluster type: ", .self$cluster,
                "\n", sep="")
        }
    )
)

BatchtoolsParam <-
    function(workers = batchtoolsWorkers(cluster),
             cluster = batchtoolsCluster())
{
    .BatchtoolsParam(workers = workers, cluster = cluster)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - control
###

setMethod("bpisup", "BatchtoolsParam",
          function(x)
          {
              TRUE
          })

setMethod("bpbackend", "BatchtoolsParam",
    function(x)
{
    x$cluster
})

setMethod("bpstart", "BatchtoolsParam",
          function(x)
{
    reg <- batchtools::makeRegistry(
        file.dir=tempfile("registry_", "."),
        conf.file = conf.file
    )
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
