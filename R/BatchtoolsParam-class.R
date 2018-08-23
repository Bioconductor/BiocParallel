### ================================================================
### BatchtoolsParam objects
### ----------------------------------------------------------------

.BATCHTOOLS_CLUSTERS <- c(
    "socket", "multicore", "interactive", "sge", "slurm", "lsf", "openlava",
    "torque"
)

### -------------------------------------------------
###  Helper functions
###

batchtoolsWorkers <-
    function(cluster = batchtoolsCluster())
{
    switch(
        match.arg(cluster, .BATCHTOOLS_CLUSTERS),
        interactive = 1L,
        socket = snowWorkers("SOCK"),
        multicore = multicoreWorkers(),
        stop("specify number of workers for '", cluster, "'")
    )
}

batchtoolsCluster <-
    function(cluster)
{
    if (missing(cluster)) {
        if (.Platform$OS.type == "windows") {
            cluster <- "socket"
        } else {
            cluster <- "multicore"
        }
    } else {
        cluster <- match.arg(cluster, .BATCHTOOLS_CLUSTERS)
    }
    cluster
}

.batchtoolsClusterAvailable <-
    function(cluster)
{
    switch(
        cluster,
        socket = TRUE,
        multicore = .Platform$OS.type != "windows",
        interactive = TRUE,
        sge = suppressWarnings(system2("qstat", stderr=NULL) != 127L),
        slurm = suppressWarnings(system2("squeue", stderr=NULL) != 127L),
        lsf = suppressWarnings(system2("bjobs", stderr=NULL) != 127L),
        openlava = suppressWarnings(system2("bjobs", stderr=NULL) != 127L),
        torque = suppressWarnings(system2("qselect", stderr=NULL) != 127L),
        stop("unsupported cluster type '", cluster, "'")
    )
}

batchtoolsTemplate <-
    function(cluster)
{
    if (!cluster %in% .BATCHTOOLS_CLUSTERS)
        stop("unsupported cluster type '", cluster, "'")
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

batchtoolsRegistryargs <- function(...) {
    args <- list(...)

    ## our defaults...
    registryargs <- as.list(formals(batchtools::makeRegistry))
    registryargs$file.dir <- tempfile(tmpdir=getwd())
    registryargs$conf.file <- registryargs$seed <- NULL
    registryargs$make.default <- FALSE

    ## ...modified by user
    registryargs[names(args)] <- args

    registryargs
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

.BatchtoolsParam <- setRefClass(
    "BatchtoolsParam",
    contains="BiocParallelParam",
    fields = list(
        cluster = "character",
        template = "character",
        registry = "Registry",
        registryargs = "list",
        resources = "list",
        RNGseed = "integer",
        logdir = "character"
    ),
    methods = list(
        show = function() {
            callSuper()
            .registryargs <- .bpregistryargs(.self)
            .resources <- .bpresources(.self)       
            cat("  cluster type: ", bpbackend(.self),
                "\n", .prettyPath("  template", .bptemplate(.self)),
                "\n  bpRNGseed: ", bpRNGseed(.self),
                "\n  bplogdir: ", bplogdir(.self),
                "\n  registryargs:",
                paste0("\n    ", names(.registryargs), ": ", .registryargs),
                "\n  resources:", 
                paste0("\n    ", names(.resources), ": ", .resources),
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
        resources = list(),
        template = batchtoolsTemplate(cluster),
        stop.on.error = TRUE,
        progressbar=FALSE, RNGseed = NA_integer_,
        timeout= 30L * 24L * 60L * 60L, log=FALSE, logdir=NA_character_,
        resultdir=NA_character_, jobname = "BPJOB"
    )
{
    if (!requireNamespace("batchtools", quietly=TRUE))
        stop("BatchtoolsParam() requires 'batchtools' package")

    if (!.batchtoolsClusterAvailable(cluster))
        stop("'", cluster, "' supported but not available on this machine")

    if (length(resources)  && is.null(names(resources)))
        stop("'resources' must be a named list")

    .BatchtoolsParam(
        workers = workers, cluster = cluster, registry = .NULLRegistry(),
        registryargs = registryargs, resources = resources,
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

.bpregistryargs <-
    function(x)
{
    x$registryargs
}

.bpresources <-
    function(x)
{
    x$resources
}

.bptemplate <-
    function(x)
{
    x$template
}

.composeBatchtools <-
    function(FUN)
{
    force(FUN)
    function(fl, ...) {
        x <- readRDS(fl)
        FUN(x, ...)
    }
}

setMethod("bpbackend", "BatchtoolsParam",
    function(x)
{
    x$cluster
})

setMethod("bpstart", "BatchtoolsParam",
    function(x)
{
    if (bpisup(x))
        return(invisible(x))

    cluster <- bpbackend(x)
    registryargs <- .bpregistryargs(x)

    oopt <- options(batchtools.verbose = FALSE)
    on.exit(options(oopt))

    seed <- bpRNGseed(x)
    if (!is.na(seed))
        registryargs$seed <- seed

    registry <- do.call(batchtools::makeRegistry, registryargs)

    registry$cluster.functions <- switch(
        cluster,
        interactive = batchtools::makeClusterFunctionsInteractive(),
        socket = batchtools::makeClusterFunctionsSocket(bpnworkers(x)),
        multicore = batchtools::makeClusterFunctionsMulticore(bpnworkers(x)),
        sge = batchtools::makeClusterFunctionsSGE(template = .bptemplate(x)),
        ## Add mutliple cluster support
        slurm = batchtools::makeClusterFunctionsSlurm(template=.bptemplate(x)),
        lsf = batchtools::makeClusterFunctionsLSF(template=.bptemplate(x)),
        openlava = batchtools::makeClusterFunctionsOpenLava(
            template=.bptemplate(x)
        ),
        torque = batchtools::makeClusterFunctionsTORQUE(
            template=.bptemplate(x)
        ),
        default = stop("unsupported cluster type '", cluster, "'")
    )

    x$registry <- registry              # toggles bpisup()
    invisible(x)
})

setMethod("bpstop", "BatchtoolsParam",
          function(x)
{
    registry <- x$registry
    message("cleaning registry...")
    suppressMessages({
        batchtools::removeRegistry(reg=registry)
    })

    x$registry <- .NULLRegistry()       # toggles bpisup()
    invisible(x)
})


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - evaluation
###

setMethod("bplapply", c("ANY", "BatchtoolsParam"),
          function(X, FUN, ..., BPREDO = list(), BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)

    if (!length(X))
        return(list())

    idx <- .redo_index(X, BPREDO)
    if (any(idx))
        X <- X[idx]
    nms <- names(X)

    ## start / stop cluster
    if (!bpisup(BPPARAM)) {
        BPPARAM <- bpstart(BPPARAM)
        on.exit(bpstop(BPPARAM), TRUE)
    }

    ## progressbar
    prev.bp <- getOption("BBmisc.ProgressBar.style")
    on.exit(options(BBmisc.ProgressBar.style=prev.pb), TRUE)

    pb <- c("off", "text")[bpprogressbar(BPPARAM) + 1L]
    prev.pb <- options(BBmisc.ProgressBar.style=pb)

    registry <- BPPARAM$registry

    FUN <- .composeTry(
        FUN, bplog(BPPARAM), bpstopOnError(BPPARAM), timeout=bptimeout(BPPARAM)
    )

    ##  Make registry / map / submit / wait / load
    ids = batchtools::batchMap(
        fun=FUN, X, more.args = list(...), reg = registry
    )
    ids$chunk = batchtools::chunk(ids$job.id, n.chunks = bpnworkers(BPPARAM))

    batchtools::submitJobs(
        ids = ids, resources = .bpresources(BPPARAM), reg = registry
    )
    batchtools::waitForJobs(
        ids = ids, reg = registry, timeout = bptimeout(BPPARAM),
        stop.on.error = bpstopOnError(BPPARAM)
    )
    res <- batchtools::reduceResultsList(ids = ids, reg = registry)

    ## Copy logs from log dir to bplogdir before clearing registry
    if (bplog(BPPARAM) && !is.na(bplogdir(BPPARAM))) {
        logs <- file.path(.bpregistryargs(BPPARAM)$file.dir, "logs")
        ## Create log dir
        dir.create(bplogdir(BPPARAM))
        ## Recursive copy logs
        file.copy(logs, bplogdir(BPPARAM) , recursive=TRUE, overwrite=TRUE)
    }

    ## Clear registry
    suppressMessages({
        batchtools::clearRegistry(reg=registry)
    })
    cat("\n")  ## terminate progress bar

    if (!is.null(res))
        names(res) <- nms

    if (any(idx)) {
        BPREDO[idx] <- res
        res <- BPREDO
    }

    if (!all(bpok(res)))
        stop(.error_bplist(res))

    res
})


setMethod("bpiterate", c("ANY", "ANY", "BatchtoolsParam"),
    function(ITER, FUN, ..., REDUCE, init, reduce.in.order=FALSE,
             BPPARAM=bpparam())
{
    ITER <- match.fun(ITER)
    FUN <- match.fun(FUN)

    if (missing(REDUCE)) {
        if (reduce.in.order)
            stop("REDUCE must be provided when 'reduce.in.order = TRUE'")
        if (!missing(init))
            stop("REDUCE must be provided when 'init' is given")
    }

    if (!bpschedule(BPPARAM) || bpnworkers(BPPARAM) == 1L) {
        param <- SerialParam(stop.on.error=bpstopOnError(BPPARAM),
                             log=bplog(BPPARAM),
                             threshold=bpthreshold(BPPARAM),
                             progressbar=bpprogressbar(BPPARAM))
        return(bpiterate(ITER, FUN, ..., REDUCE=REDUCE, init=init,
                         BPPARAM=param))
    }

    ## start / stop cluster
    if (!bpisup(BPPARAM)) {
        bpstart(BPPARAM)
        on.exit(bpstop(BPPARAM))
    }

    ## composeTry
    FUN <- .composeTry(FUN, bplog(BPPARAM), bpstopOnError(BPPARAM),
                       timeout=bptimeout(BPPARAM))

    FUN <- .composeBatchtools(FUN)

    ## Call batchtoolsIterate with arguments
    bploop(structure(list(), class="iterate_batchtools"),
           ITER, FUN, BPPARAM, REDUCE, init, reduce.in.order, ...)
})
