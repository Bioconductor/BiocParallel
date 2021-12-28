### =========================================================================
### BatchJobsParam objects
### -------------------------------------------------------------------------

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

.BatchJobsParam_prototype <- c(
    list(
        reg.pars = list(),
        submit.pars = list(),
        conf.pars = list(),
        cleanup = logical()
    ),
    .BiocParallelParam_prototype
)

.BatchJobsParam <- setRefClass("BatchJobsParam",
    contains="BiocParallelParam",
    fields=list(
        reg.pars="list",
        submit.pars="list",
        conf.pars="list",
        cleanup="logical"),
    methods=list(
        show = function() {
            ## TODO more output
            callSuper()
            cat("  cleanup: ", .self$cleanup, "\n", sep="")
        })
)

BatchJobsParam <-
    function(workers=NA_integer_, cleanup=TRUE,
        work.dir=getwd(), stop.on.error=TRUE, seed=NULL, resources=NULL,
        conffile=NULL, cluster.functions=NULL,
        progressbar=TRUE, jobname = "BPJOB",
        reg.pars=list(seed=seed, work.dir=work.dir),
        conf.pars=list(conffile=conffile, cluster.functions=cluster.functions),
        submit.pars=list(resources=resources), ...)
{
    if (!requireNamespace("BatchJobs", quietly=TRUE))
        stop("BatchJobsParam() requires the 'BatchJobs' package")

    not_null <- Negate(is.null)
    reg.pars <- Filter(not_null, reg.pars)
    submit.pars <- Filter(not_null, submit.pars)
    conf.pars <- Filter(not_null, conf.pars)

    ## save user config and reset it on exit
    prev.config <- BatchJobs::getConfig()
    on.exit(do.call(BatchJobs::setConfig, prev.config))
    if (!is.null(conf.pars$conffile))
        BatchJobs::loadConfig(conf.pars$conffile)
    new.conf <- unclass(do.call(
        BatchJobs::setConfig,
        conf.pars[setdiff(names(conf.pars), "conffile")]
    ))
    workers <-
        if (is.na(workers)) {
            getNumberCPUs <- function(conf) {
                x <-
                    environment(new.conf$cluster.functions$submitJob)$workers
                vapply(x, "[[", integer(1L), "ncpus")
            }
            cf.name <- new.conf$cluster.functions$name
            if (is.null(cf.name)) {
                NA_integer_
            } else {
                switch(cf.name, Multicore=getNumberCPUs(new.conf),
                       SSH=sum(getNumberCPUs(new.conf)), NA_integer_)
            }
        } else as.integer(workers)

    prototype <- .prototype_update(
        .BatchJobsParam_prototype,
        reg.pars=reg.pars, submit.pars=submit.pars,
        conf.pars=conf.pars, workers=workers,
        cleanup=cleanup,
        stop.on.error=stop.on.error,
        progressbar=progressbar, jobname=jobname
    )

    do.call(.BatchJobsParam, prototype)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - control
###

setMethod("bpschedule", "BatchJobsParam",
    function(x)
{
    !getOption("BatchJobs.on.slave", FALSE)
})

setMethod("bpisup", "BatchJobsParam", function(x) TRUE)

setMethod("bpbackend", "BatchJobsParam", function(x) BatchJobs::getConfig())

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - evaluation
###

setMethod("bplapply", c("ANY", "BatchJobsParam"),
    function(X, FUN, ..., BPREDO=list(), BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    BPREDO <- bpresult(BPREDO)

    if (!length(X))
        return(.rename(list(), X))

    if (!bpschedule(BPPARAM))
        return(bplapply(X, FUN, ..., BPPARAM=SerialParam()))

    idx <- .redo_index(X, BPREDO)
    if (length(idx))
        X <- X[idx]
    nms <- names(X)

    ## restore current settings
    prev.bp <- getOption("BBmisc.ProgressBar.style")
    on.exit(options(BBmisc.ProgressBar.style=prev.pb))

    prev.config <- BatchJobs::getConfig()
    on.exit(BatchJobs::setConfig(conf=prev.config), add=TRUE)


    pb <- c("off", "text")[bpprogressbar(BPPARAM)+1L]
    prev.pb <- options(BBmisc.ProgressBar.style=pb)

    ## switch config
    BatchJobs::setConfig(conf=BPPARAM$conf.pars)

    ## reg.pars
    reg.pars <- c(list(id=bpjobname(BPPARAM), skip=FALSE), BPPARAM$reg.pars)
    if (is.null(reg.pars$file.dir))
        reg.pars$file.dir <-
            file.path(reg.pars$work.dir, tempfile("BiocParallel_tmp_", ""))
    if (BPPARAM$cleanup)
        on.exit(unlink(reg.pars$file.dir, recursive=TRUE), add=TRUE)

    OPTIONS <- .workerOptions(
        log = bplog(BPPARAM),
        stop.on.error = bpstopOnError(BPPARAM),
        timeout = bptimeout(BPPARAM),
        exportglobals = bpexportglobals(BPPARAM),
        as.error = FALSE
    )

    ## FUN
    FUN <- .composeTry(FUN, OPTIONS = OPTIONS, SEED = NULL)
    WRAP <- function(.x, .FUN, .MoreArgs)
        do.call(.FUN, c(list(.x), .MoreArgs))

    res <- suppressMessages({
        ## make / map / submit / wait/ load
        reg <- do.call(BatchJobs::makeRegistry, reg.pars)
        ids <- BatchJobs::batchMap(
            reg, WRAP, X, more.args=list(.FUN=FUN, .MoreArgs=list(...)))

        submit.pars <- c(list(reg=reg), BPPARAM$submit.pars)
        submit.pars$ids <- if (is.na(bpnworkers(BPPARAM))) {
            ids
        } else BBmisc::chunk(ids, n.chunks=bpnworkers(BPPARAM), shuffle=TRUE)
        do.call(BatchJobs::submitJobs, submit.pars)

        BatchJobs::waitForJobs(reg, ids, timeout=.Machine$integer.max,
                               stop.on.error=bpstopOnError(BPPARAM))
        BatchJobs::loadResults(reg, ids, use.names="none")
    })

    ## post-process
    names(res) <- nms

    if (length(BPREDO) && length(idx)) {
        BPREDO[idx] <- res
        res <- BPREDO
    }

    ok <- bpok(res)
    if (!all(ok)) {
        ## HACK: promote conditions to errors
        res[!ok] <- lapply(res[!ok], function(x) {
            class(x) <- c(class(x)[-length(class(x))], c("error", "condition"))
            x
        })
        stop(.error_bplist(res))
    }

    res
})

setMethod("bpiterate", c("ANY", "ANY", "BatchJobsParam"),
    function(ITER, FUN, ..., BPREDO = list(), BPPARAM=bpparam())
{
    stop("bpiterate not supported for BatchJobsParam")
})
