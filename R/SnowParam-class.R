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
        .rng_internal_stream$set()
        on.exit(.rng_internal_stream$reset())
        portAvailable <- FALSE
        for (i in 1:5) {
            port <- as.integer(
                11000 +
                1000 * ((stats::runif(1L) + unclass(Sys.time()) / 300) %% 1L)
            )
            tryCatch(
                {
                    ## User should not be able to interrupt the port check
                    ## Otherwise we might have an unclosed connection
                    suspendInterrupts(
                        {
                            con <- serverSocket(port)
                            close(con)
                        }
                    )
                    portAvailable <- TRUE
                },
                error = function(e) {
                    message("failed to open the port ", port,", trying a new port...")
                }
            )
            if (portAvailable) break
        }
        if (!portAvailable)
            .stop("cannot find an open port. For manually specifying the port, see ?SnowParam")
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
                      timeout=WORKER_TIMEOUT,
                      exportglobals=TRUE, exportvariables=TRUE,
                      log=FALSE, threshold="INFO", logdir=NA_character_,
                      resultdir=NA_character_, jobname = "BPJOB",
                      force.GC = FALSE,
                      fallback = TRUE,
                      manager.hostname=NA_character_,
                      manager.port=NA_integer_, ...)
{
    type <- tryCatch(match.arg(type), error=function(...) {
        stop("'type' must be one of ",
             paste(sQuote(formals("SnowParam")$type), collapse=", "))
    })

    if (type %in% c("MPI", "FORK") && is(workers, "character"))
        stop("'workers' must be integer(1) when 'type' is MPI or FORK")

    if (progressbar && missing(tasks))
        tasks <- TASKS_MAXIMUM

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
        timeout=as.integer(timeout),
        exportglobals=exportglobals,
        exportvariables=exportvariables,
        log=log, threshold=threshold, logdir=logdir,
        resultdir=resultdir, jobname=jobname,
        force.GC = force.GC,
        fallback = fallback,
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

.bpstart_makeCluster <-
    function(cargs)
{
    ## set internal stream to avoid iterating global random number
    ## stream in `parallel::makeCluster()`. Use the internal stream so
    ## that the random number generator advances on each call.
    state <- .rng_internal_stream$set()
    on.exit(.rng_internal_stream$reset())
    do.call(parallel::makeCluster, cargs)
}

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
        socket_timeout <- as.integer(getOption("timeout"))
        socket_timeout_is_valid <-
            length(socket_timeout) == 1L && !is.na(socket_timeout) &&
            socket_timeout > 0L
        if (!socket_timeout_is_valid)
            stop("'getOption(\"timeout\")' must be positive integer(1)")
        bpbackend(x) <- .bpfork(nnodes, .hostname(x), .port(x), socket_timeout)
    ## SOCK, MPI
    } else {
        cargs <- x$.clusterargs
        cargs$spec <- if (is.numeric(cargs$spec)) {
            nnodes
        } else cargs$spec[seq_len(nnodes)]

        ## work around devtools::load_all()
        ##
        ## 'inst' exists when using devtools::load_all()
        libPath <- find.package("BiocParallel")
        if (dir.exists(file.path(libPath, "inst")))
            libPath <- file.path(libPath, "inst")

        if (is.null(cargs$snowlib))
            cargs$snowlib <- libPath

        if (!is.null(cargs$useRscript) && !cargs$useRscript)
            cargs$scriptdir <- libPath

        if (x$.clusterargs$type == "SOCK") {
            cargs$master <- .hostname(x)
            cargs$port <- .port(x)
        }

        bpbackend(x) <- .bpstart_makeCluster(cargs)
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
    x$log <- value
    x
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

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### task dispatching interface
###

setOldClass(c("SOCK0node", "SOCKnode")) # needed for method dispatch

.SOCKmanager <- setClass("SOCKmanager", contains = "TaskManager")

setMethod(
    ".manager", "SnowParam",
    function(BPPARAM)
{
    manager <- callNextMethod()
    manager$initialized <- rep(FALSE, manager$capacity)
    as(manager, "SOCKmanager")
})

setMethod(
    ".manager_send", "SOCKmanager",
    function(manager, value, ...)
{
    availability <- manager$availability
    stopifnot(length(availability) >= 0L)
    ## send the job to the next available worker
    worker <- names(availability)[1]
    id <- as.integer(worker)
    ## Do the cache only when the snow worker is
    ## created by our package.
    if (.controlled(manager$BPPARAM)) {
        if (manager$initialized[id])
            value <- .task_dynamic(value)
        else
            manager$initialized[id] <- TRUE
    }
    .send_to(manager$backend, as.integer(worker), value)
    rm(list = worker, envir = availability)
    manager
})

setMethod(
    ".manager_cleanup", "SOCKmanager",
    function(manager)
{
    manager <- callNextMethod()
    manager$initialized <- rep(FALSE, manager$capacity)
    if (.controlled(manager$BPPARAM)) {
        value <- .EXEC(tag = NULL, .clean_task_static, args = NULL)
        .send_all(manager$backend, value)
        msg <- .recv_all(manager$backend)
    }
    manager
})

## The worker class of SnowParam
setMethod(
    ".recv", "SOCKnode",
    function(worker)
{
    msg <- callNextMethod()
    if (inherits(msg, "error"))
        return(msg)
    ## read/write the static value(if any)
    .load_task_static(msg)
})
