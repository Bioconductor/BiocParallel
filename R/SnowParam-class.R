### =========================================================================
### SnowParam objects
### -------------------------------------------------------------------------

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Helpers
###

.snowHost <- function(local=TRUE) {
    host <-
        if (local) {
            "localhost"
        } else Sys.info()[["nodename"]]
    host <- Sys.getenv("MASTER", host)
    host <- getOption("bphost", host)

    host
}

.snowPort <- function() {
    port <- Sys.getenv("R_PARALLEL_PORT", NA_integer_)
    port <- Sys.getenv("PORT", port)
    port <- getOption("ports", port)

    if (identical(tolower(port), "random") || is.na(port)) {
        octx <- .internal_rng_stream$set()
        on.exit(.internal_rng_stream$unset(octx))
        port <- as.integer(
            11000 +
            1000 * ((stats::runif(1L) + unclass(Sys.time()) / 300) %% 1L)
        )
    } else {
        port <- as.integer(port)
    }

    port
}

.snowCoresMax <- function(type) {
    if (type == "MPI") {
        .Machine$integer.max
    } else {
        128L - nrow(showConnections(all=TRUE))
    }
}

.snowCores <- function(type) {
    if (type == "multicore" && .Platform$OS.type == "windows")
        return(1L)

    min(.detectCores(), .snowCoresMax(type))
}

snowWorkers <- function(type = c("SOCK", "MPI", "FORK")) {
    type <- match.arg(type)
    .snowCores(type)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

setOldClass(c("NULLcluster", "cluster"))

.NULLcluster <-
    function()
{
    cl <- list()
    class(cl) <- c("NULLcluster", "cluster")
    cl
}

.SnowParam_prototype <- c(
    list(
        cluster = .NULLcluster(),
        .clusterargs = list(spec=0, type="SOCK"),
        .controlled = TRUE,
        hostname = NA_character_, port = NA_integer_
    ),
    .BiocParallelParam_prototype
)

.SnowParam <- setRefClass("SnowParam",
    contains="BiocParallelParam",
    fields=list(
        cluster = "cluster",
        .clusterargs = "list",
        .controlled = "logical",
        hostname = "character",
        port = "integer"
    ),
    methods=list(
        show = function() {
            callSuper()
            cat("  cluster type: ", .clusterargs$type, "\n", sep="")
        })
)

SnowParam <- function(workers=snowWorkers(type),
                      type=c("SOCK", "MPI", "FORK"), tasks=0L,
                      stop.on.error=TRUE,
                      progressbar=FALSE, RNGseed=NULL,
                      timeout=30L * 24L * 60L * 60L,
                      exportglobals=TRUE,
                      log=FALSE, threshold="INFO", logdir=NA_character_,
                      resultdir=NA_character_, jobname = "BPJOB",
                      manager.hostname=NA_character_,
                      manager.port=NA_integer_, ...)
{
    type <- tryCatch(match.arg(type), error=function(...) {
        stop("'type' must be one of ",
             paste(sQuote(formals("SnowParam")$type), collapse=", "))
    })

    if (type %in% c("MPI", "FORK") && is(workers, "character"))
        stop("'workers' must be integer(1) when 'type' is MPI or FORK")

    clusterargs <- c(list(spec=workers, type=type), list(...))

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
        .SnowParam_prototype,
        .clusterargs=clusterargs,
        .controlled=TRUE, workers=workers, tasks=as.integer(tasks),
        stop.on.error=stop.on.error,
        progressbar=progressbar, RNGseed=RNGseed,
        timeout=as.integer(timeout), exportglobals=exportglobals,
        log=log, threshold=threshold, logdir=logdir,
        resultdir=resultdir, jobname=jobname,
        hostname=manager.hostname, port=manager.port,
        ...
    )

    do.call(.SnowParam, prototype)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Validity
###

setValidity("SnowParam", function(object)
{
    msg <- NULL
    if (!.isTRUEorFALSE(.controlled(object)))
        msg <- c(msg, "'.controlled' must be TRUE or FALSE")

    if (.controlled(object)) {
        if (!all(bpworkers(object) == object$.clusterargs$spec))
            msg <- c(msg,
                "'bpworkers(BPPARAM)' must equal BPPARAM$.clusterargs$spec")
    }

    if (is.null(msg)) TRUE else msg
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Getters / Setters
###

.hostname <- function(x)
    x$hostname

.port <- function(x)
    x$port

setReplaceMethod("bpworkers", c("SnowParam", "numeric"),
    function(x, value)
{
    value <- as.integer(value)
    max <- .snowCoresMax(x$.clusterargs$type)
    if (value > max)
        stop("'value' exceeds ", max, " available workers; see ?snowWorkers")

    x$workers <- value
    x$.clusterargs$spec <- value
    x
})

setReplaceMethod("bpworkers", c("SnowParam", "character"),
    function(x, value)
{
    x$workers <- x$.clusterargs$spec <- value
    x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - control
###

setMethod("bpstart", "SnowParam",
    function(x, lenX = bpnworkers(x))
{
    if (!.controlled(x))
        stop("'bpstart' not available; instance from outside BiocParallel?")
    if (bpisup(x))
        stop("cluster already started")
    if (bpnworkers(x) == 0 && lenX <= 0)
        stop("cluster not started; no workers specified")

    nnodes <- min(bpnworkers(x), lenX)
    if (x$.clusterargs$type != "MPI" &&
        (nnodes > 128L - nrow(showConnections(all=TRUE))))
        stop("cannot create ", nnodes, " workers; ",
             128L - nrow(showConnections(all=TRUE)),
             " connections available in this session")

    ## FORK (useRscript not relevant)
    if (x$.clusterargs$type == "FORK") {
        bpbackend(x) <- .bpfork(nnodes, bptimeout(x), .hostname(x), .port(x))
    ## SOCK, MPI
    } else {
        cargs <- x$.clusterargs
        cargs$spec <- if (is.numeric(cargs$spec)) {
            nnodes
        } else cargs$spec[seq_len(nnodes)]
        if (is.null(cargs$snowlib))
            cargs$snowlib <- find.package("BiocParallel")
        if (!is.null(cargs$useRscript) && !cargs$useRscript)
            cargs$scriptdir <- find.package("BiocParallel")

        if (x$.clusterargs$type == "SOCK") {
            cargs$master <- .hostname(x)
            cargs$port <- .port(x)
        }

        bpbackend(x) <- do.call(parallel::makeCluster, cargs)
    }

    .bpstart_impl(x)
})

setMethod("bpstop", "SnowParam",
    function(x)
{
    if (!.controlled(x)) {
        warning("'bpstop' not available; instance from outside BiocParallel?")
        return(invisible(x))
    }
    if (!bpisup(x))
        return(invisible(x))

    x <- .bpstop_impl(x)
    cluster <- bpbackend(x)
    for (i in seq_along(cluster))
        .close(cluster[[i]])
    bpbackend(x) <- .NULLcluster()

    invisible(x)
})

setMethod("bpisup", "SnowParam",
    function(x)
{
    length(bpbackend(x)) != 0L
})

setMethod("bpbackend", "SnowParam",
    function(x)
{
    x$cluster
})

setReplaceMethod("bpbackend", c("SnowParam", "cluster"),
    function(x, value)
{
    x$cluster <- value
    x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Getters / Setters
###

.controlled <-
    function(x)
{
    x$.controlled
}

setReplaceMethod("bplog", c("SnowParam", "logical"),
    function(x, value)
{
    if (x$.controlled) {
        x$log <- value
        if (bpisup(x)) {
            bpstop(x)
            bpstart(x)
        }
        x
    } else {
        stop("'bplog' not available; instance from outside BiocParallel?")
    }
})

setReplaceMethod("bpthreshold", c("SnowParam", "character"),
    function(x, value)
{
    x$threshold <- value
    x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion methods for SOCK and MPI clusters
###

### parallel::SOCKcluster types

setOldClass(c("SOCKcluster", "cluster"))

stopCluster.SOCKcluster <-
    parallel:::stopCluster.default

setAs("SOCKcluster", "SnowParam",
    function(from)
{
    .clusterargs <-
        list(spec=length(from), type=sub("cluster$", "", class(from)[1L]))
    prototype <- .prototype_update(
        .SnowParam_prototype,
        .clusterargs = .clusterargs,
        cluster = from,
        .controlled = FALSE,
        workers = length(from)
    )

    do.call(.SnowParam, prototype)
})

### MPIcluster

setOldClass(c("spawnedMPIcluster", "MPIcluster", "cluster"))

setAs("spawnedMPIcluster", "SnowParam",
    function(from)
{
    .clusterargs <- list(spec=length(from),
        type=sub("cluster", "", class(from)[1L]))
    .SnowParam(.clusterargs=.clusterargs, cluster=from, .controlled=FALSE,
               workers=length(from))
})
