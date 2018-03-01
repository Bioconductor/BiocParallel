## TODO: Support more arguments from BiocParallelPram, jobname, tasks,
## TODO: bplapply
## TODO: updated unit tests
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
    fields = list(
        cluster = "character",
        registry = "Registry",
        jobname = "character",
        progressbar = "logical",
        ## FIXME: composeTry
        log = "logical",
        stop.on.error = "logical",
        timeout = "integer"
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
             cluster = batchtoolsCluster(), stop.on.error = TRUE,
             jobname = "BPJOB",
             ## FIXME: Not sure why we need to have log either
             log = FALSE,
             ## 1 week == 604800 sec, Number taken from batchtools waitForJobs
             timeout = 7L * 24L * 60L * 60L,
             progressbar = FALSE
             )
{
    if (!requireNamespace("batchtools", quietly=TRUE)) {
        stop("BatchtoolsParam() requires 'batchtools' package")
    }

    .BatchtoolsParam(
        workers = workers, cluster = cluster, registry = .NULLRegistry(),
        jobname = jobname,progressbar = progressbar,
        ## FIXME: These are taken by composeTry and batchtools::submitJobs
        log = log, stop.on.error = stop.on.error, timeout = timeout
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

    ## progressbar
    prev.bp <- getOption("BBmisc.ProgressBar.style")
    on.exit(options(BBmisc.ProgressBar.style=prev.pb))

    pb <- c("off", "text")[bpprogressbar(BPPARAM)+1L]
    prev.pb <- options(BBmisc.ProgressBar.style=pb)

    registry <- BPPARAM$registry
    ##  Make registry / map / submit / wait / load
    ids = batchtools::batchMap(fun=FUN, X, more.args = list(...), reg = registry)
    ids$chunk = batchtools::chunk(ids$job.id, n.chunks = bpnworkers(BPPARAM))

    batchtools::submitJobs(ids = ids, resources = list(), reg = registry)
    batchtools::waitForJobs(ids = ids, reg = registry,
                            timeout = bptimeout(BPPARAM),
                            stop.on.error = bpstopOnError(BPPARAM))
    result <- batchtools::reduceResultsList(ids = ids, reg = registry)
    ## Clear registry
    batchtools::clearRegistry(reg=registry)

    result
})
