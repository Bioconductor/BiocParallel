## TODO: Support more arguments from BiocParallelPram, tasks
##   (? maybe max.concurrent.jobs is really bpworkers(), tasks is
##   chunk.size?)
## TODO: bplapply
## TODO: updated unit tests
## TODO: Support more makeClusterFunction* and makeRegistry args
## TODO: Support more cluster back-ends, e.g., SGE
##   - maybe templates in inst/batchtools/; batchtoolTemplates()
## TODO: fix progress bar

## CHALLEGING: implement BPREDO for bplapply; bpiterate()

### ================================================================
### BatchtoolsParam objects
### ----------------------------------------------------------------

.BATCHTOOLS_CLUSTERS <- c("socket", "multicore", "interactive", "sge")

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

.BatchtoolsParam <- setRefClass(
    "BatchtoolsParam",
    contains="BiocParallelParam",
    fields = list(
        cluster = "character",
        template = "character",
        registry = "Registry",
        RNGseed = "integer",
        logdir = "character"
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
        cluster = batchtoolsCluster(),
        template = NA_character_,
        stop.on.error = TRUE,
        progressbar=FALSE, RNGseed = NA_integer_,
        timeout= 30L * 24L * 60L * 60L, log=FALSE, logdir=NA_character_,
        resultdir=NA_character_, jobname = "BPJOB"
    )
{
    if (!requireNamespace("batchtools", quietly=TRUE))
        stop("BatchtoolsParam() requires 'batchtools' package")

    ## Check if template is valid
    if (missing(template)) {
        template <- batchtoolsTemplate(cluster)
    } else {
        template <- template
    }

    .BatchtoolsParam(
        workers = workers, cluster = cluster, registry = .NULLRegistry(),
        jobname = jobname, progressbar = progressbar, log = log,
        logdir = logdir, stop.on.error = stop.on.error, timeout = timeout,
        RNGseed = RNGseed, template = template
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

    registry <- batchtools::makeRegistry(
        file.dir = tempfile(), conf.file = character(),
        make.default = FALSE, seed = seed
    )

    registry$cluster.functions <- switch(
        cluster,
        interactive = batchtools::makeClusterFunctionsInteractive(),
        socket = batchtools::makeClusterFunctionsSocket(bpnworkers(x)),
        multicore = batchtools::makeClusterFunctionsMulticore(bpnworkers(x)),
        sge = {
            template <- batchtoolsTemplate(cluster)
            batchtools::makeClusterFunctionsSGE(template = template)
        },
        ## slurm, lsf, torque, openlava
        default = stop("unsupported cluster type '", cluster, "'")
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

### -------------------------------------------------
###  Helper function to return correct template
###
batchtoolsTemplate <-
    function(cluster)
{
    ## cluster should accept template from only
    ## sge, slurm, lsf, torque, openlava
    condition <- !file.exists(cluster)

    ## Check if template and cluster are valid
    if (condition) {
        tmpl <- sprintf("batchtools-%s.tmpl", tolower(cluster))
        return(system.file("batchtools", tmpl, package="BiocParallel"))
    } else {
        return(cluster)
    }
}
