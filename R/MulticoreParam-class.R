### =========================================================================
### MulticoreParam objects
### -------------------------------------------------------------------------

multicoreWorkers <- function()
    .snowCores("multicore")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor 
###

.MulticoreParam_prototype <- .SnowParam_prototype

.MulticoreParam <- setRefClass("MulticoreParam",
    contains="SnowParam",
    fields=list(),
    methods=list()
)

MulticoreParam <- function(workers=multicoreWorkers(), tasks=0L,  
        stop.on.error=TRUE, 
        progressbar=FALSE, RNGseed=NULL, timeout= 30L * 24L * 60L * 60L,
        exportglobals=TRUE,
        log=FALSE, threshold="INFO", logdir=NA_character_,
        resultdir=NA_character_, jobname = "BPJOB",
        manager.hostname=NA_character_, manager.port=NA_integer_, ...)
{
    if (.Platform$OS.type == "windows") {
        warning("MulticoreParam() not supported on Windows, use SnowParam()")
        workers = 1L
    }

    if (progressbar && missing(tasks))
        tasks <- .Machine$integer.max

    clusterargs <- c(list(spec=workers, type="FORK"), list(...))

    manager.hostname <-
        if (is.na(manager.hostname)) {
            local <- (clusterargs$type == "FORK") ||
                is.numeric(clusterargs$spec)
            manager.hostname <- .snowHost(local)
        } else as.character(manager.hostname)

    manager.port <-
        if (is.na(manager.port)) {
            .snowPort()
        } else as.integer(manager.port)

    if (!is.null(RNGseed))
        RNGseed <- as.integer(RNGseed)

    prototype <- .prototype_update(
        .MulticoreParam_prototype,
        .clusterargs=clusterargs, cluster=.NULLcluster(),
        .controlled=TRUE, workers=as.integer(workers), 
        tasks=as.integer(tasks),
        stop.on.error=stop.on.error, 
        progressbar=progressbar, 
        RNGseed=RNGseed, timeout=as.integer(timeout),
        exportglobals=exportglobals,
        log=log, threshold=threshold,
        logdir=logdir, resultdir=resultdir, jobname=jobname,
        hostname=manager.hostname, port=manager.port,
        ...
    )

    do.call(.MulticoreParam, prototype)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - control
###

setReplaceMethod("bpworkers", c("MulticoreParam", "numeric"),
    function(x, value)
{
    value <- as.integer(value)
    max <- .snowCoresMax("multicore")
    if (value > max)
        stop(
            "'value' exceeds ", max, " available workers; see ?multicoreWorkers"
        )
 
    x$workers <- value 
    x$.clusterargs$spec <- value 
    x 
})

setMethod("bpschedule", "MulticoreParam",
    function(x)
{
    if (.Platform$OS.type == "windows") 
        FALSE
    else
        TRUE
})
