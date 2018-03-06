## TODO: Support more arguments from BiocParallelPram, tasks
##   (? maybe max.concurrent.jobs is really bpworkers(), tasks is
##   chunk.size?), log / logdir (copy from registry??)
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

setMethod("show", "NULLRegistry",
    function(object)
{
    cat("NULL Job Registry\n")
})

.BatchtoolsParam <- setRefClass(
    "BatchtoolsParam",
    contains="BiocParallelParam",
    fields = list(
        cluster = "character",
        registry = "Registry",
        RNGseed = "ANY",
        logdir = "character"
    ),
    methods = list(
        show = function() {
            callSuper()
            cat("  cluster type: ", .self$cluster,
                "\n",
                "  bpRNGseed: ", .self$RNGseed,
                "\n",
                "  bplogdir: ", .self$logdir,
                "\n", sep="")
        }
    )
)

BatchtoolsParam <-
    function(
        workers = batchtoolsWorkers(cluster),
        cluster = batchtoolsCluster(), stop.on.error = TRUE,
        progressbar=FALSE, RNGseed=NULL,
        timeout= 30L * 24L * 60L * 60L, log=FALSE, logdir=NA_character_,
        resultdir=NA_character_, jobname = "BPJOB"
    )
{
    if (!requireNamespace("batchtools", quietly=TRUE))
        stop("BatchtoolsParam() requires 'batchtools' package")

    if(!is.null(RNGseed)) {
        if (!is(RNGseed,"numeric"))
            stop("'RNGseed' needs to be numeric value")
    }

    .BatchtoolsParam(
        workers = workers, cluster = cluster, registry = .NULLRegistry(),
        jobname = jobname, progressbar = progressbar, log = log,
        logdir = logdir, stop.on.error = stop.on.error, timeout = timeout,
        RNGseed = RNGseed
    )
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Validity
###

.valid.BatchtoolsParam.log <- .valid.SnowParam.log

setValidity("BatchtoolsParam", function(object)
{
    msg <- NULL
    if(!is.na(object$log))
        msg <- c(msg,
                 .valid.BatchtoolsParam.log(object))

    if (is.null(msg))
        TRUE
    else
        msg
})


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - control
###

setMethod("bplogdir", "BatchtoolsParam",
    function(x)
{
    x$logdir
})

setReplaceMethod("bplogdir", c("BatchtoolsParam", "character"),
    function(x, value)
{
    if (!length(value)) {
        ##        if (bpisup(x))
        ##           value <- x$registry$file.dir
        ##        else
        value <- NA_character_
    }
    x$logdir <- value
    if (is.null(msg <- .valid.BatchtoolsParam.log(x)))
        x
    else
        stop(msg)
})

setMethod("bpisup", "BatchtoolsParam",
    function(x)
{
    !is(x$registry, "NULLRegistry")
})

setMethod("bpRNGseed", "BatchtoolsParam",
    function(x)
{
    x$RNGseed
})

setReplaceMethod("bpRNGseed", c("BatchtoolsParam", "numeric"),
    function(x, value)
{
    if (bpisup(x)) {
        message("'bpRNGseed()' does not support being reset being started with",
                " 'bpstart()'.\n Use 'bpstop()' before resetting the 'bpRNGseed()'")
    } else if (!bpisup(x)) {
        x$RNGseed <- as.integer(value)
    }
    x
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
        make.default = FALSE, seed=x$RNGseed
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

    pb <- c("off", "text")[bpprogressbar(BPPARAM) + 1L]
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

    ## Copy logs from log dir to bplogdir before clearing registry
    if (bplog(BPPARAM) && !is.na(bplogdir(BPPARAM))) {

        logs <- file.path(BPPARAM$registry$file.dir, "logs")
        ## Create log dir
        dir.create(bplogdir(BPPARAM))
        ## Recursive copy logs
        file.copy(logs, bplogdir(BPPARAM) , recursive=TRUE,
                  overwrite=TRUE)
    }

    ## Clear registry
    suppressMessages({
        batchtools::clearRegistry(reg=registry)
        cat("\n")                       # terminate progress bar
    })

    result
})
