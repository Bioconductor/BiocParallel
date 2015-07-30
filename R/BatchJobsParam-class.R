### =========================================================================
### BatchJobsParam objects
### -------------------------------------------------------------------------

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor 
###

.BatchJobsParam <- setRefClass("BatchJobsParam",
    contains="BiocParallelParam",
    fields=list(
        reg.pars="list",
        submit.pars="list",
        conf.pars="list",
        cleanup="logical"),
    methods=list(
        initialize = function(..., 
            conf.pars=list(), 
            workers=NA_integer_) 
        {
            callSuper(...)

            ## save user config and reset it on exit
            prev.config <- getConfig()
            on.exit(do.call(setConfig, prev.config))
            if (!is.null(conf.pars$conffile))
                loadConfig(conf.pars$conffile)
            new.conf <- unclass(do.call(setConfig,
                conf.pars[setdiff(names(conf.pars), "conffile")]))
            x_workers <- if (is.na(workers)) {
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

            initFields(workers=x_workers, conf.pars=new.conf)
        },
        show = function() {
            ## TODO more output
            callSuper()
            cat("  bpjobname:", bpjobname(.self), 
                   "; bpworkers:", bpworkers(.self), 
                   "; bpisup:", bpisup(.self), "\n", sep="")
            cat("  bpstopOnError:", bpstopOnError(.self),
                   "; bpprogressbar:", bpprogressbar(.self), "\n", sep="")
            cat("  cleanup:", .self$cleanup, "\n", sep="")
        })
)

BatchJobsParam <-
    function(workers=NA_integer_, catch.errors=TRUE, cleanup=TRUE,
        work.dir=getwd(), stop.on.error=FALSE, seed=NULL, resources=NULL,
        conffile=NULL, cluster.functions=NULL,
        progressbar=TRUE, jobname = "BPJOB", ...)
{
    if (!catch.errors)
        warning("'catch.errors' has been deprecated")
    if (!"package:BatchJobs" %in% search()) {
        tryCatch({
            attachNamespace("BatchJobs")
        }, error=function(err) {
            stop(conditionMessage(err), 
                ": BatchJobsParam class objects require the 'BatchJobs' package")
        })
    }

    not_null <- Negate(is.null)
    reg.pars <- Filter(not_null, list(seed=seed, work.dir=work.dir))
    submit.pars <- Filter(not_null, list(resources=resources))
    conf.pars <- Filter(not_null,
        list(conffile=conffile, cluster.functions=cluster.functions))

    .BatchJobsParam(reg.pars=reg.pars, submit.pars=submit.pars,
                    conf.pars=conf.pars, workers=workers, 
                    catch.errors=catch.errors, cleanup=cleanup, 
                    stop.on.error=stop.on.error, 
                    progressbar=progressbar, jobname=jobname, ...)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - control
###

setMethod(bpschedule, "BatchJobsParam",
    function(x, ...)
{
    !getOption("BatchJobs.on.slave", FALSE)
})

setMethod(bpisup, "BatchJobsParam", function(x, ...) TRUE)

setMethod(bpbackend, "BatchJobsParam", function(x, ...) getConfig())

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - evaluation
###

setMethod(bplapply, c("ANY", "BatchJobsParam"),
    function(X, FUN, ..., BPREDO=list(), BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    if (length(BPREDO)) {
        if (all(idx <- !bpok(BPREDO)))
            stop("no error detected in 'BPREDO'")
        if (length(BPREDO) != length(X))
            stop("Cannot resume: length mismatch in arguments")
        message("Resuming previous calculation ... ")
        X <- X[idx]
    }
    nms <- names(X)

    if (!length(X))
        return(list())
    if (!bpschedule(BPPARAM)) 
        return(bplapply(X, FUN, ..., BPPARAM=SerialParam()))

    ## turn progressbar on/off
    prev.pb <- getOption("BBmisc.ProgressBar.style")
    options(BBmisc.ProgressBar.style=c("off", "text")[bpprogressbar(BPPARAM)+1L])
    on.exit(options(BBmisc.ProgressBar.style=prev.pb))

    ## create registry, handle cleanup
    file.dir <- file.path(BPPARAM$reg.pars$work.dir,
        tempfile("BiocParallel_tmp_", ""))
    pars <- c(list(id=bpjobname(BPPARAM), file.dir=file.dir, skip=FALSE),
        BPPARAM$reg.pars)
    reg <- suppressMessages(do.call("makeRegistry", pars))
    if (BPPARAM$cleanup)
        on.exit(unlink(file.dir, recursive=TRUE), add=TRUE)

    ## switch config
    prev.config <- getConfig()
    on.exit(setConfig(conf=prev.config), add=TRUE)
    setConfig(conf=BPPARAM$conf.pars)

    ## error handling; sanity check
    if (BPPARAM$catch.errors)
        FUN <- .composeTry(FUN)
    if (BPPARAM$stop.on.error && BPPARAM$catch.errors) {
        txt <- strwrap("options 'stop.on.error' and 'catch.errors' are
            both set, but are mutually exclusive; disabling 'stop.on.error'",
            indent=4, exdent=4)
        warning(paste(txt, collapse="\n"))
    }

    ## package args for batchMap
    wrap <- function(.x, .FUN, .MoreArgs) {
        do.call(.FUN, c(list(.x), .MoreArgs))
    }
    ## define jobs and submit, possibly chunked
    ids <- suppressMessages(batchMap(reg, fun=wrap, X, 
                            more.args=list(.FUN=FUN, .MoreArgs=list(...))))
    pars <- c(list(reg=reg), BPPARAM$submit.pars)
    if (is.na(BPPARAM$workers))
        pars$ids <- ids
    else
        pars$ids <- BBmisc::chunk(ids, n.chunks=BPPARAM$workers, shuffle=TRUE)
    suppressMessages(do.call(submitJobs, pars))

    # wait for the jobs to terminate
    waitForJobs(reg, ids, timeout=Inf,
        stop.on.error=(BPPARAM$stop.on.error && !BPPARAM$catch.errors))

    ## FIXME: pass USE.NAMES?
    res <- loadResults(reg, ids, use.names="none")
    if (!is.null(res)) {
        names(res) <- nms
    }
    if (length(BPREDO)) {
        BPREDO[idx] <- res
        BPREDO 
    } else res
})

setMethod(bpiterate, c("ANY", "ANY", "BatchJobsParam"),
    function(ITER, FUN, ..., BPPARAM=bpparam())
{
    stop(paste0("bpiterate not supported for BatchJobsParam"))
})
