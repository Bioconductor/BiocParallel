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


setMethod("bpworkers", "BatchtoolsParam",
    function(x)
{
    x$workers
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
        make.default = FALSE
    )
    registry$cluster.functions <- switch(
        cluster,
        interactive = batchtools::makeClusterFunctionsInteractive(),
        socket = batchtools::makeClusterFunctionsSocket(bpnworkers(x)),
        multicore = batchtools::makeClusterFunctionsMulticore(bpnworkers(x))
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
    FUN <- match.fun(FUN)

    if (!length(X))
        return(list())

    ## start / stop cluster
    if (!bpisup(BPPARAM)) {
        BPPARAM <- bpstart(BPPARAM)
        on.exit(bpstop(BPPARAM), TRUE)
    }

    registry <- BPPARAM$registry
    ##  Make registry / map / submit / wait / load
    ids = batchtools::batchMap(fun=FUN, X=X, more.args = list(...), reg = registry)
    ids$chunk = batchtools::chunk(ids$job.id, n.chunks = BPPARAM$workers)

    batchtools::submitJobs(ids = ids, reg = registry)
    batchtools::waitForJobs(ids = ids, reg = registry)
    result <- batchtools::reduceResultsList(ids = ids, reg = registry)
    ## Clear registry
    batchtools::clearRegistry(reg=registry)
    
    result
})


