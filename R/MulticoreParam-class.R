### =========================================================================
### MulticoreParam objects
### -------------------------------------------------------------------------

multicoreWorkers <- function()
    .snowCores("multicore")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor 
###

.MulticoreParam <- setRefClass("MulticoreParam",
    contains="SnowParam",
    fields=list(),
    methods=list()
)

MulticoreParam <- function(workers=multicoreWorkers(), tasks=0L,  
        catch.errors=TRUE, stop.on.error=TRUE, 
        progressbar=FALSE, RNGseed=NULL, timeout= 30L * 24L * 60L * 60L,
        exportglobals=TRUE,
        log=FALSE, threshold="INFO", logdir=NA_character_,
        resultdir=NA_character_, jobname = "BPJOB",
        manager.hostname=NA_character_, manager.port=NA_integer_, ...)
{
    if (.Platform$OS.type == "windows")
        warning("MulticoreParam() not supported on Windows, use SnowParam()")

    if (!missing(catch.errors))
        warning("'catch.errors' is deprecated, use 'stop.on.error'")

    args <- c(list(spec=workers, type="FORK"), list(...)) 
    .MulticoreParam(.clusterargs=args, cluster=.NULLcluster(),
        .controlled=TRUE, workers=as.integer(workers), 
        tasks=as.integer(tasks),
        catch.errors=catch.errors, stop.on.error=stop.on.error, 
        progressbar=progressbar, 
        RNGseed=RNGseed, timeout=timeout, exportglobals=exportglobals,
        log=log, threshold=threshold,
        logdir=logdir, resultdir=resultdir, jobname=jobname,
        hostname=manager.hostname, port=manager.port)
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
