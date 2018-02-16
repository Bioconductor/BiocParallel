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

setOldClass(c("NULLRegistry", "Registry"))

.NULLRegistry <-
    function()
{
    structure(list(), class=c("NULLRegistry", "Registry"))
}

.BatchtoolsParam <- setRefClass(
    "BatchtoolsParam",
    contains="BiocParallelParam",
    field = list(
        cluster = "character",
        registry = "Registry"
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
    .BatchtoolsParam(
        workers = workers, cluster = cluster, registry = .NULLRegistry()
    )
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - control
###

setMethod("bpisup", "BatchtoolsParam",
    function(x)
{
    !is(x$registry, "NULLRegistry")
})

setMethod("bpbackend", "BatchtoolsParam",
    function(x)
{
    x$cluster
})

setMethod("bpstart", "BatchtoolsParam",
    function(x)
{
    cluster <- bpbackend(x)

    oopt <- options(batchtools.verbose = FALSE)
    on.exit(options(batchtools.verbose = oopt$batchtools.verbose))

    registry <- makeRegistry(
        file.dir = tempfile(), conf.file = character(),
        make.default = FALSE
    )
    registry$cluster.functions <- switch(
        cluster,
        interactive = makeClusterFunctionsInteractive(),
        socket = makeClusterFunctionsSocket(bpnworkers(x)),
        multicore = makeClusterFunctionsMulticore(bpnworkers(x))
    )

    x$registry <- registry
    invisible(x)
})

setMethod("bpstop", "BatchtoolsParam",
          function(x)
{
    registry <- x$registry
    batchtools::removeRegistry(reg=registry)

    x$registry <- .NULLRegistry()
    invisible(x)
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
