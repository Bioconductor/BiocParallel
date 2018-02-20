## TODO: bplapply
## TODO: updated unit tests
## TODO: Support more BiocParallel-class parameters, e.g., jobname
## TODO: Support more makeClusterFunction* and makeRegistry args
## TODO: Support more cluster back-ends, e.g., SGE
##   - maybe templates in inst/batchtools/; batchtoolTemplates()

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
    ## TODO: bpisup(x) for interactive is TRUE
    ## if (identical(bpbackend(x), "interactive")) {
    ##     TRUE
    ## }
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

    registry <- batchtools::makeRegistry(
        file.dir = tempfile(), conf.file = character(),
        ## Make registry default TRUE
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
    browser()
    FUN <- match.fun(FUN)

    if (!length(X))
        return(list())


    if (!bpisup(BPPARAM)) {
        stop("`bpisup()` is FALSE. Start the param using `bpstart()` before using `bplapply()`")
    }
    registry <- BPPARAM$registry
    res <- suppressMessages({
        ##  Make registry / map / submit / wait / load

        if (is(registry, "NULLRegistry")) {
            stop("Call 'bpstart()' on your BatchtoolsParam object before using 'bplapply()'")
        }
        workers <- bpnworkers(BPPARAM)
        ids <- batchtools::batchMap(fun=FUN,
                                    n=workers,
                                    reg=registry)

        batchtools::submitJobs(ids, reg=registry)

        batchtools::waitForJobs(reg=registry, ids=ids)
        ##stop.on.error=bpstopOnError(BPPARAM))

        ## loadResults
        lapply(seq_len(workers), batchtools::loadResult,
               id=getJobTable(reg=registry),
               reg=registry)

    })
    ## Make invisible call to clear registry
    batchtools::clearRegistry(reg=registry)
})

## Workers and how we are passing them in
