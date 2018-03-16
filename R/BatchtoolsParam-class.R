## TODO: Support more arguments from BiocParallelPram, tasks
##   (? maybe max.concurrent.jobs is really bpworkers(), tasks is
##   chunk.size?)
## TODO: updated unit tests
## TODO: fix progress bar
## CHALLEGING TODO: implement BPREDO for bplapply; bpiterate()

### ================================================================
### BatchtoolsParam objects
### ----------------------------------------------------------------

.BATCHTOOLS_CLUSTERS <- c("socket", "multicore", "interactive", "sge",
                          "slurm", "lsf", "openlava", "torque")

batchtoolsWorkers <-
    function(cluster = .BATCHTOOLS_CLUSTERS)
{
    switch(
        match.arg(cluster),
        interactive = 1L,
        .snowCores(multicore=.Platform$OS.type != "windows")
    )
}

batchtoolsCluster <-
    function(cluster = .BATCHTOOLS_CLUSTERS)
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

print.NULLRegistry <-
    function(x, ...)
{
    cat("NULL Job Registry\n")
}

setOldClass("ClusterFunctions")

batchtoolsRegistryargs <- function(...) {
    args <- list(...)
    registryargs <- as.list(formals(batchtools::makeRegistry))
    registryargs[names(args)] <- args
    registryargs
}

.BatchtoolsParam <- setRefClass(
    "BatchtoolsParam",
    contains="BiocParallelParam",
    fields = list(
        cluster = "character",
        template = "character",
        registry = "Registry",
        registryargs = "list",
        RNGseed = "integer",
        logdir = "character",
        .cluster.functions = "ClusterFunctions"
    ),
    methods = list(
        show = function() {
            callSuper()
            cat("  cluster type: ", .self$cluster,
                "\n",
                "  template: ", .self$template,
                "\n",
                "  bpRNGseed: ", bpRNGseed(.self),
                "\n",
                "  bplogdir: ", bplogdir(.self),
                "\n", sep="")
        }
    )
)

BatchtoolsParam <-
    function(
        workers = batchtoolsWorkers(cluster),
        ## Provide either cluster or template
        cluster = batchtoolsCluster(),
        registryargs = batchtoolsRegistryargs(),
        template = batchtoolsTemplate(cluster),
        stop.on.error = TRUE,
        progressbar=FALSE, RNGseed = NA_integer_,
        timeout= 30L * 24L * 60L * 60L, log=FALSE, logdir=NA_character_,
        resultdir=NA_character_, jobname = "BPJOB"
    )
{
    if (!requireNamespace("batchtools", quietly=TRUE))
        stop("BatchtoolsParam() requires 'batchtools' package")

    .cluster.functions <- switch(
        cluster,
        interactive = batchtools::makeClusterFunctionsInteractive(),
        socket = batchtools::makeClusterFunctionsSocket(workers),
        multicore = batchtools::makeClusterFunctionsMulticore(workers),
        sge = batchtools::makeClusterFunctionsSGE(template = template),
        ## Add mutliple cluster support
        slurm = batchtools::makeClusterFunctionsSlurm(template=template),
        lsf = batchtools::makeClusterFunctionsLSF(template=template),
        openlava = batchtools::makeClusterFunctionsOpenLava(template=template),
        torque = batchtools::makeClusterFunctionsTORQUE(template=template),
        default = stop("unsupported cluster type '", cluster, "'")
    )

    .BatchtoolsParam(
        workers = workers, cluster = cluster, registry = .NULLRegistry(),
        registryargs = registryargs,
        jobname = jobname, progressbar = progressbar, log = log,
        logdir = logdir, stop.on.error = stop.on.error, timeout = timeout,
        RNGseed = RNGseed, template = template, .cluster.functions = .cluster.functions
    )
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Validity
###

.valid.BatchtoolsParam.log <- .valid.SnowParam.log

setValidity("BatchtoolsParam", function(object)
{
    msg <- NULL
    if (!bpbackend(object) %in% .BATCHTOOLS_CLUSTERS) {
        types <- paste(.BATCHTOOLS_CLUSTERS, collape = ", ")
        msg <- c(msg, paste("'cluster' must be one of", types))
    }
    if(.isTRUEorFALSE(bplog(object)))
        msg <- c(msg, .valid.BatchtoolsParam.log(object))

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
    if (bpisup(x))
        stop("use 'bpstop()' before setting 'bplogdir()'")

    x$logdir <- value
    validObject(x)
    x
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
    if (bpisup(x))
        stop("use 'bpstop()' before setting 'bpRNGseed()'")

    x$RNGseed <- as.integer(value)
    x
})

setMethod("bptemplate", "BatchtoolsParam",
    function(x)
{
    x$template
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

    seed <- bpRNGseed(x)
    if (is.na(seed))
        seed <- NULL

    x$registryargs$conf.file = character()
    x$registryargs$make.default = FALSE
    x$registryargs$seed = seed

    registry <- do.call(batchtools::makeRegistry, x$registryargs)

    registry$cluster.functions <- x$.cluster.functions

    x$registry <- registry
    invisible(x)
})

setMethod("bpstop", "BatchtoolsParam",
          function(x)
{
    ## TODO: bpstop for cluster types not available on
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
        cat("\n")  ## terminate progress bar
    })

    result
})

### -------------------------------------------------
###  Helper function to return correct template
###
batchtoolsTemplate <-
    function(cluster)
{
    if (!cluster %in% .BATCHTOOLS_CLUSTERS)
        stop("unsupported cluster type.")
    if (cluster %in% c("socket", "multicore", "interactive"))
        return(NA_character_)

    message("using default '", cluster, "' template in batchtools.")
    if (cluster == "torque")
        tmpl <- "torque-lido.tmpl"
    else
        tmpl <- sprintf("%s-simple.tmpl", tolower(cluster))
    ## return template
    system.file("templates", tmpl, package="batchtools")
}
