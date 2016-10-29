### =========================================================================
### SnowParam objects
### -------------------------------------------------------------------------

.SnowClusters <- local({
    ## package-global registry of backends; use to avoid closing
    ## socket connections of unreferenced backends during garbage
    ## collection -- bpstart(MulticoreParam(1)); gc(); gc()
    uid <- 0
    env <- environment()
    list(add = function(backend) {
        uid <<- uid + 1L
        cuid <- as.character(uid)
        env[[cuid]] <- backend          # protection
        cuid
    }, drop = function(cuid) {
        rm(list=cuid, envir=env)
        invisible(NULL)
    }, get = function(cuid) {
        env[[cuid]]
    }, ls = function() {
        cuid <- setdiff(ls(env), c("uid", "env"))
        cuid[order(as.integer(cuid))]
    })
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

snowWorkers <- function() {
    cores <- max(1L, parallel::detectCores() - 2L)
    if (nzchar(Sys.getenv("BBS_HOME")))
        cores <- min(4L, cores)
    getOption("mc.cores", cores)
}

setOldClass(c("NULLcluster", "cluster"))

.NULLcluster <-
    function()
{
    cl <- list()
    class(cl) <- c("NULLcluster", "cluster")
    cl
}

.SnowParam <- setRefClass("SnowParam",
    contains="BiocParallelParam",
    fields=list(
        cluster="cluster",
        .clusterargs="list",
        .controlled="logical",
        .uid = "character",
        RNGseed="ANY",
        logdir="character",
        resultdir="character",
        finalizer_env="environment"),
    methods=list(
        initialize = function(...,
            .clusterargs=list(spec=0, type="SOCK"),
            .controlled=TRUE,
            RNGseed=NULL,
            logdir=NA_character_,
            resultdir=NA_character_)
        {
            env <- as.environment(list(self=.self))
            reg.finalizer(env, function(e) {
                if (.controlled(e[["self"]]))
                    bpstop(e[["self"]])
            }, onexit=TRUE)
            callSuper(...)
            initFields(.clusterargs=.clusterargs, .controlled=.controlled,
                       RNGseed=RNGseed, logdir=logdir, resultdir=resultdir,
                       finalizer_env=env)
        },
        show = function() {
            callSuper()
            cat("  bpRNGseed: ", bpRNGseed(.self),
                "\n",
                "  bplogdir: ", bplogdir(.self),
                "\n",
                "  bpresultdir: ", bpresultdir(.self),
                "\n",
                "  cluster type: ", .clusterargs$type,
                "\n", sep="")
        })
)

SnowParam <- function(workers=snowWorkers(),
                      type=c("SOCK", "MPI", "FORK"), tasks=0L,
                      catch.errors=TRUE, stop.on.error=TRUE,
                      progressbar=FALSE, RNGseed=NULL,
                      timeout= 30L * 24L * 60L * 60L,
                      log=FALSE, threshold="INFO", logdir=NA_character_,
                      resultdir=NA_character_, jobname = "BPJOB", ...)
{
    if (!missing(catch.errors))
        warning("'catch.errors' is deprecated, use 'stop.on.error'")

    type <- tryCatch(match.arg(type), error=function(...) {
        stop("'type' must be one of ",
             paste(sQuote(formals("SnowParam")$type), collapse=", "))
    })

    if (type %in% c("MPI", "FORK") && is(workers, "character"))
        stop("'workers' must be integer(1) when 'type' is MPI or FORK")

    args <- c(list(spec=workers, type=type), list(...))
    x <- .SnowParam(.clusterargs=args, cluster=.NULLcluster(),
                    .controlled=TRUE, workers=workers, tasks=as.integer(tasks),
                    catch.errors=catch.errors, stop.on.error=stop.on.error,
                    progressbar=progressbar, RNGseed=RNGseed, timeout=timeout,
                    log=log, threshold=threshold, logdir=logdir,
                    resultdir=resultdir, jobname=jobname)
    validObject(x)
    x
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Validity
###

.valid.SnowParam.log <- function(object)
{
    msg <- NULL

    dir <- bplogdir(object)
    if (length(dir) > 1L || !is(dir, "character"))
        msg <- c(msg, "'bplogdir(BPPARAM)' must be character(1)")

    if (!is.na(dir) && !bplog(object))
        msg <- c(msg, "'log' must be TRUE when 'logdir' is given")
    msg
}

.valid.SnowParam.result <- function(object) {
    msg <- NULL

    dir <- bpresultdir(object)
    if (length(dir) > 1L || !is(dir, "character"))
        msg <- c(msg, "'bpresultdir(BPPARAM)' must be character(1)")

    msg
}

setValidity("SnowParam", function(object)
{
    msg <- NULL
    if (!.isTRUEorFALSE(.controlled(object)))
        msg <- c(msg, "'.controlled' must be TRUE or FALSE")

    if (.controlled(object)) {
        if (!all(bpworkers(object) == object$.clusterargs$spec))
            msg <- c(msg,
                "'bpworkers(BPPARAM)' must equal BPPARAM$.clusterargs$spec")
        msg <- c(msg,
                 .valid.SnowParam.log(object),
                 .valid.SnowParam.result(object))
    }

    if (is.null(msg)) TRUE else msg
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Getters / Setters
###

setReplaceMethod("bpworkers", c("SnowParam", "numeric"),
    function(x, value)
{
    value <- as.integer(value)
    if (value > snowWorkers())
        stop("'value' exceeds available workers detected by snowWorkers()")

    x$workers <- value
    x$.clusterargs$spec <- value
    x
})

setMethod("bpRNGseed", "SnowParam",
    function(x)
{
    x$RNGseed
})

setReplaceMethod("bpRNGseed", c("SnowParam", "numeric"),
    function(x, value)
{
    x$RNGseed <- as.integer(value)
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
        bpbackend(x) <-
            .bpmakeForkCluster(nnodes, bptimeout(x),
                               getOption("bphost", Sys.info()[["nodename"]]),
                               getOption("ports", NA_integer_))
    ## SOCK, MPI
    } else {
        cargs <- x$.clusterargs
        cargs$spec <- if (is.numeric(cargs$spec)) {
            nnodes
        } else cargs$spec[seq_len(nnodes)]
        cargs$snowlib <- find.package("BiocParallel")
        if (!is.null(cargs$useRscript) && !cargs$useRscript)
            cargs$scriptdir <- find.package("BiocParallel")
        bpbackend(x) <- do.call(parallel::makeCluster, cargs)
    }

    ## logging
    if (bplog(x))
        .initiateLogging(x)

    ## random numbers
    tryCatch({
        if (!is.null(bpRNGseed(x)))
            parallel::clusterSetRNGStream(bpbackend(x), bpRNGseed(x))
    }, error = function(err) {
        bpstop(x)
        stop("setting worker RNG stream:\n  ", conditionMessage(err))
    })

    ## timeout
    tryCatch({
        timeout <- bptimeout(x)
        if (is.finite(timeout)) {
            parallel::clusterExport(bpbackend(x), "timeout", env=environment())
        }
    }, error = function(err) {
        bpstop(x)
        stop("setting worker timeout:\n  ", conditionMessage(err))
    })

    ## don't let the connections close
    x$.uid <- .SnowClusters$add(bpbackend(x))

    invisible(x)
})

setMethod("bpstop", "SnowParam",
    function(x)
{
    if (!.controlled(x))
        stop("'bpstop' not available; instance from outside BiocParallel?")
    if (!bpisup(x))
        return(invisible(x))

    tryCatch({
        parallel::stopCluster(bpbackend(x))
    }, error=function(err) {
        txt <- sprintf("failed to stop %s cluster: %s",
                       sQuote(class(bpbackend(x))[[1]]),
                       conditionMessage(err))
        stop(paste(strwrap(txt, exdent=2), collapse="\n"), call.=FALSE)
    })
    bpbackend(x) <- .NULLcluster()
    .SnowClusters$drop(x$.uid)
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
### Methods - evaluation
###

setMethod("bplapply", c("ANY", "SnowParam"),
    function(X, FUN, ..., BPREDO=list(), BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)

    if (!is.na(bpresultdir(BPPARAM)))
        BatchJobs:::checkDir(bpresultdir(BPPARAM))

    if (bplog(BPPARAM) && !is.na(bplogdir(BPPARAM)))
        BatchJobs:::checkDir(bplogdir(BPPARAM))

    if (!length(X))
        return(list())

    if (!bpschedule(BPPARAM) || length(X) == 1L || bpnworkers(BPPARAM) == 1L) {
        param <- SerialParam(stop.on.error=bpstopOnError(BPPARAM),
                             log=bplog(BPPARAM),
                             threshold=bpthreshold(BPPARAM))
        return(bplapply(X, FUN, ..., BPREDO=BPREDO, BPPARAM=param))
    }

    idx <- .redo_index(X, BPREDO)
    if (any(idx))
        X <- X[idx]
    nms <- names(X)

    ## start / stop cluster
    if (!bpisup(BPPARAM)) {
        BPPARAM <- bpstart(BPPARAM, length(X))
        on.exit(bpstop(BPPARAM), TRUE)
    }

    ## FUN
    FUN <- .composeTry(FUN, bplog(BPPARAM), bpstopOnError(BPPARAM),
                       timeout=bptimeout(BPPARAM))

    ## split into tasks
    X <- .splitX(X, bpnworkers(BPPARAM), bptasks(BPPARAM))
    ARGFUN <- function(i) c(list(X=X[[i]]), list(FUN=FUN), list(...))

    res <- bploop(structure(list(), class="lapply"), # dispatch
                  X, lapply, ARGFUN, BPPARAM)

    if (!is.null(res)) {
        res <- do.call(unlist, list(res, recursive=FALSE))
        names(res) <- nms
    }

    if (any(idx)) {
        BPREDO[idx] <- res
        res <- BPREDO
    }

    if (!all(bpok(res)))
        stop(.error_bplist(res))

    res
})

setMethod("bpiterate", c("ANY", "ANY", "SnowParam"),
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

    if (!is.na(bpresultdir(BPPARAM)))
        BatchJobs:::checkDir(bpresultdir(BPPARAM))

    if (bplog(BPPARAM) && !is.na(bplogdir(BPPARAM)))
        BatchJobs:::checkDir(bplogdir(BPPARAM))

    if (!bpschedule(BPPARAM) || bpnworkers(BPPARAM) == 1L) {
        param <- SerialParam(stop.on.error=bpstopOnError(BPPARAM),
                             log=bplog(BPPARAM),
                             threshold=bpthreshold(BPPARAM))
        return(bpiterate(ITER, FUN, ..., REDUCE=REDUCE, init=init,
                         BPPARAM=param))
    }

    ## start / stop cluster
    if (!bpisup(BPPARAM)) {
        BPPARAM <- bpstart(BPPARAM)
        on.exit(bpstop(BPPARAM), TRUE)
    }

    ## FUN
    FUN <- .composeTry(FUN, bplog(BPPARAM), bpstopOnError(BPPARAM),
                       timeout=bptimeout(BPPARAM))
    ARGFUN <- function(value) c(list(value), list(...))

    ## FIXME: handle errors via bpok()
    bploop(structure(list(), class="iterate"), # dispatch
           ITER, FUN, ARGFUN, BPPARAM, REDUCE, init, reduce.in.order)
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

setMethod("bplogdir", "SnowParam",
    function(x)
{
    x$logdir
})

setReplaceMethod("bplogdir", c("SnowParam", "character"),
    function(x, value)
{
    if (!length(value))
        value <- NA_character_
    x$logdir <- value
    if (is.null(msg <- .valid.SnowParam.log(x)))
        x
    else
        stop(msg)
})

setMethod("bpresultdir", "SnowParam",
    function(x)
{
    x$resultdir
})

setReplaceMethod("bpresultdir", c("SnowParam", "character"),
    function(x, value)
{
    x$resultdir <- value
    if (is.null(msg <- .valid.SnowParam.result(x)))
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
    .clusterargs <- list(spec=length(from),
        type=sub("cluster$", "", class(from)[1L]))
    .SnowParam(.clusterargs=.clusterargs, cluster=from,
        .controlled=FALSE, workers=length(from))
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
