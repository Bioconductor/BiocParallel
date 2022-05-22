message("Testing BatchtoolsParam")

.old_options <- NULL

.setUp <- function()
    .old_options <<- options(BIOCPARALLEL_BATCHTOOLS_REMOVE_REGISTRY_WAIT = 1)

.tearDown <- function() {
    options(.old_options)
}

.n_connections <- function() {
    gc()                                # close connections
    nrow(showConnections())
}

test_BatchtoolsParam_constructor <- function() {
    param <- BatchtoolsParam()
    checkTrue(validObject(param))

    checkTrue(is(param$registry, "NULLRegistry"))

    isWindows <- .Platform$OS.type == "windows"
    cluster <- if (isWindows) "socket" else "multicore"
    nworkers <- if (isWindows) snowWorkers() else multicoreWorkers()
    checkIdentical(cluster, bpbackend(param))
    checkIdentical(nworkers, bpnworkers(param))

    checkIdentical(3L, bpnworkers(BatchtoolsParam(3L)))

    cluster <- "socket"
    param <- BatchtoolsParam(cluster=cluster)
    checkIdentical(cluster, bpbackend(param))
    checkIdentical(nworkers, bpnworkers(param))

    cluster <- "multicore"
    if (BiocParallel:::.batchtoolsClusterAvailable(cluster)) {
        param <- BatchtoolsParam(cluster=cluster)
        checkIdentical(cluster, bpbackend(param))
        checkIdentical(nworkers, bpnworkers(param))
    }

    cluster <- "interactive"
    if (BiocParallel:::.batchtoolsClusterAvailable(cluster)) {
        param <- BatchtoolsParam(cluster=cluster)
        checkIdentical(cluster, bpbackend(param))
        checkIdentical(1L, bpnworkers(param))
    }

    cluster <- "sge"
    if (BiocParallel:::.batchtoolsClusterAvailable(cluster)) {
        param <- BatchtoolsParam(workers=2, cluster=cluster)
        checkIdentical(cluster, bpbackend(param))
    }

    cluster <- "lsf"
    if (BiocParallel:::.batchtoolsClusterAvailable(cluster)) {
        param <- BatchtoolsParam(workers=2, cluster=cluster)
        checkIdentical(cluster, bpbackend(param))
    }

    cluster <- "slurm"
    if (BiocParallel:::.batchtoolsClusterAvailable(cluster)) {
        param <- BatchtoolsParam(workers=2, cluster=cluster)
        checkIdentical(cluster, bpbackend(param))
    }

    cluster <- "openlava"
    if (BiocParallel:::.batchtoolsClusterAvailable(cluster)) {
        param <- BatchtoolsParam(workers=2, cluster=cluster)
        checkIdentical(cluster, bpbackend(param))
    }

    cluster <- "torque"
    if (BiocParallel:::.batchtoolsClusterAvailable(cluster)) {
        param <- BatchtoolsParam(workers=2, cluster=cluster)
        checkIdentical(cluster, bpbackend(param))
    }

    cluster <- "unknown"
    checkException(BatchtoolsParam(cluster=cluster))
}

test_BatchtoolsWorkers <- function() {
    socket <- snowWorkers()
    multicore <- multicoreWorkers()
    isWindows <- .Platform$OS.type == "windows"

    checkIdentical(
        if (isWindows) socket else multicore,
        batchtoolsWorkers()
    )
    checkIdentical(socket, batchtoolsWorkers("socket"))
    cluster <- "multicore"
    if (BiocParallel:::.batchtoolsClusterAvailable(cluster))
        checkIdentical(multicore, batchtoolsWorkers(cluster))

    checkIdentical(1L, batchtoolsWorkers("interactive"))

    checkException(batchtoolsWorkers("unknown"))
}

.test_BatchtoolsParam_bpisup_start_stop <- function(param) {
    n_connections <- .n_connections()

    checkIdentical(FALSE, bpisup(param))
    checkIdentical(TRUE, bpisup(bpstart(param)))
    checkIdentical(FALSE, bpisup(bpstop(param)))
    checkIdentical(n_connections, .n_connections())
}

test_BatchtoolsParam_bpisup_start_stop_default <- function() {

    param <- BatchtoolsParam(workers=2)
    .test_BatchtoolsParam_bpisup_start_stop(param)
}

test_BatchtoolsParam_bpisup_start_stop_socket <- function() {
    cluster <- "socket"
    param <- BatchtoolsParam(workers=2, cluster=cluster)
    checkIdentical(cluster, bpbackend(param))
    .test_BatchtoolsParam_bpisup_start_stop(param)
}

test_BatchtoolsParam_bpisup_start_stop_interactive <- function() {
    cluster <- "interactive"
    param <- BatchtoolsParam(workers=2, cluster=cluster)
    checkIdentical(cluster, bpbackend(param))
    .test_BatchtoolsParam_bpisup_start_stop(param)
}

test_BatchtoolsParam_bplapply <- function() {
    n_connections <- .n_connections()
    fun <- function(x) Sys.getpid()

    ## Check for all cluster types
    cluster <-  "interactive"
    param <- BatchtoolsParam(workers=2, cluster=cluster)
    result <- bplapply(1:5, fun, BPPARAM=param)
    checkIdentical(1L, length(unique(unlist(result))))

    cluster <- "multicore"
    if (BiocParallel:::.batchtoolsClusterAvailable(cluster)) {
        param <- BatchtoolsParam(workers=2, cluster=cluster)
        result <- bplapply(1:5, fun, BPPARAM=param)
        checkIdentical(2L, length(unique(unlist(result))))
    }

    cluster <- "socket"
    param <- BatchtoolsParam(workers=2, cluster=cluster)
    result <- bplapply(1:5, fun, BPPARAM=param)
    checkIdentical(2L, length(unique(unlist(result))))
    checkIdentical(n_connections, .n_connections())
}

## Check registry
test_BatchtoolsParam_registry <- function() {
    n_connections <- .n_connections()

    param <- BatchtoolsParam()
    checkTrue(is(param$registry, "NULLRegistry"))
    bpstart(param)
    checkTrue(!is(param$registry, "NULLRegistry"))
    checkTrue(is(param$registry, "Registry"))
    bpstop(param)
    checkIdentical(n_connections, .n_connections())
}

## Check bpjobname
test_BatchtoolsParam_bpjobname <- function() {
    checkIdentical("BPJOB", bpjobname(BatchtoolsParam()))
    checkIdentical("myjob", bpjobname(BatchtoolsParam(jobname="myjob")))
}

## Check bpstopOnError
test_BatchtoolsParam_bpstopOnError <- function() {
    checkTrue(bpstopOnError(BatchtoolsParam()))
    checkIdentical(FALSE, bpstopOnError(BatchtoolsParam(stop.on.error=FALSE)))
}

## Check bptimeout
test_BatchtoolsParam_bptimeout <- function() {
    checkEquals(BiocParallel:::WORKER_TIMEOUT, bptimeout(BatchtoolsParam()))
    checkEquals(123L, bptimeout(BatchtoolsParam(timeout=123)))
}

## Check bpRNGseed
test_BatchtoolsParam_bpRNGseed <- function() {
    n_connections <- .n_connections()

    ##  Check setting RNGseed
    param <- BatchtoolsParam(RNGseed=123L)
    checkEqualsNumeric(123L, bpRNGseed(param))
    ## Check reset RNGseed
    new_seed <- 234L
    bpRNGseed(param) <- new_seed
    checkEqualsNumeric(new_seed, bpRNGseed(param))
    ## Check after bpstart
    bpstart(param)
    checkEqualsNumeric(new_seed, bpRNGseed(param))
    checkEqualsNumeric(new_seed, param$registry$seed)
    bpstop(param)
    ## Check failure to reset
    ## ## Check NULL value
    param <- BatchtoolsParam()
    checkTrue(is.na(bpRNGseed(param)))
    ## ## Check fail
    checkException({bpRNGseed(param) <- "abc"})
    checkIdentical(n_connections, .n_connections())
}

test_BatchtoolsParam_bplog <- function() {
    n_connections <- .n_connections()

    ## Test param w/o log and logdir
    checkTrue(is.na(bplogdir(BatchtoolsParam())))
    checkTrue(!bplog(BatchtoolsParam()))
    ## test param with log, w/o logdir
    param <- BatchtoolsParam(log=TRUE)
    checkTrue(bplog(param))
    checkTrue(is.na(bplogdir(param)))
    ## Check if setter works
    temp_log_dir <- tempfile()
    dir.create(temp_log_dir)
    bplogdir(param) <- temp_log_dir
    checkIdentical(temp_log_dir, bplogdir(param))
    ## test param without log and w logdir
    checkException(BatchtoolsParam(logdir=temp_log_dir))
    ## check logs in logdir
    param <- BatchtoolsParam(log=TRUE, logdir=temp_log_dir)
    bplapply(1:5, sqrt, BPPARAM=param)
    checkTrue(file.exists(temp_log_dir))
    checkTrue(file.exists(file.path(temp_log_dir, "logs")))
    checkIdentical(n_connections, .n_connections())
}

test_BatchtoolsParam_available_clusters <- function() {
    clusters <- BiocParallel:::.BATCHTOOLS_CLUSTERS
    checkTrue(all.equal(
        c("socket", "multicore", "interactive", "sge",
          "slurm", "lsf", "openlava", "torque"),
        clusters))
}

test_BatchtoolsParam_template <- function() {
    .bptemplate <- BiocParallel:::.bptemplate
    cluster <- "socket"
    param <- BatchtoolsParam(cluster=cluster)
    checkTrue(is.na(.bptemplate(param)))

    cluster <- "multicore"
    if (BiocParallel:::.batchtoolsClusterAvailable(cluster)) {
        param <- BatchtoolsParam(cluster=cluster)
        checkTrue(is.na(.bptemplate(param)))
    }

    cluster <- "interactive"
    param <- BatchtoolsParam(cluster=cluster)
    checkTrue(is.na(.bptemplate(param)))

    ## Test clusters with template
    cluster <- "sge"
    if (BiocParallel:::.batchtoolsClusterAvailable(cluster)) {
        param <- BatchtoolsParam(workers=2, cluster=cluster)
        checkIdentical("sge-simple.tmpl", basename(.bptemplate(param)))
    }

    cluster <- "slurm"
    if (BiocParallel:::.batchtoolsClusterAvailable(cluster)) {
        param <- BatchtoolsParam(workers=2, cluster=cluster)
        checkIdentical("slurm-simple.tmpl", basename(.bptemplate(param)))
    }

    cluster <- "lsf"
    if (BiocParallel:::.batchtoolsClusterAvailable(cluster)) {
        param <- BatchtoolsParam(workers=2, cluster=cluster)
        checkIdentical("lsf-simple.tmpl", basename(.bptemplate(param)))
    }

    cluster <- "openlava"
    if (BiocParallel:::.batchtoolsClusterAvailable(cluster)) {
        param <- BatchtoolsParam(workers=2, cluster=cluster)
        checkIdentical("openlava-simple.tmpl", basename(.bptemplate(param)))
    }

    cluster <- "torque"
    if (BiocParallel:::.batchtoolsClusterAvailable(cluster)) {
        param <- BatchtoolsParam(workers=2, cluster=cluster)
        checkIdentical("torque-lido.tmpl", basename(.bptemplate(param)))
    }

    ## Check setting template to file path
    cluster <- "sge"
    if (BiocParallel:::.batchtoolsClusterAvailable(cluster)) {
        template <- system.file(
            "templates", "sge-simple.tmpl", package="batchtools"
        )
        param <- BatchtoolsParam(
            workers=2, cluster=cluster, template=template
        )
        checkIdentical(template, .bptemplate(param))
    }
}

## Run only of SGE clusters, this will fail on other machines
test_BatchtoolsParam_sge <- function() {
    n_connections <- .n_connections()

    if (!BiocParallel:::.batchtoolsClusterAvailable("sge"))
        return()

    fun <- function(x) Sys.getpid()

    template <- system.file(
        package="BiocParallel", "unitTests", "test_script",
        "test-sge-template.tmpl"
    )
    param <- BatchtoolsParam(workers=2,
                             cluster="sge",
                             template=template)
    bpstart(param)
    checkIdentical("SGE", param$registry$backend)

    result <- bplapply(1:5, fun, BPPARAM=param)
    checkIdentical(2L, length(unique(unlist(result))))

    bpstop(param)
    checkIdentical(n_connections, .n_connections())
}

## TODO: write tests for other cluster types, slurm, lsf, torque, openlava

test_BatchtoolsParam_bpmapply <- function() {
    n_connections <- .n_connections()
    fun <- function(x, y, z) x + y + z
    ## Initial test
    param <- BatchtoolsParam()
    result <- bpmapply(fun, x = 1:3, y = 1:3, MoreArgs = list(z = 1),
             SIMPLIFY = TRUE, BPPARAM = param)
    checkIdentical(c(3,5,7), result)

    cluster <-  "interactive"
    param <- BatchtoolsParam(workers=2, cluster=cluster)
    result <- bpmapply(fun, x = 1:3, y = 1:3, MoreArgs = list(z = 1),
                       SIMPLIFY = TRUE, BPPARAM=param)
    checkIdentical(c(3,5,7), result)

    cluster <- "multicore"
    if (BiocParallel:::.batchtoolsClusterAvailable(cluster)) {
        param <- BatchtoolsParam(workers=2, cluster=cluster)
        result <- bpmapply(fun, x = 1:3, y = 1:3, MoreArgs = list(z = 1),
                           SIMPLIFY = TRUE, BPPARAM=param)
    checkIdentical(c(3,5,7), result)
    }

    cluster <- "socket"
    param <- BatchtoolsParam(workers=2, cluster=cluster)
    result <- bpmapply(fun, x = 1:3, y = 1:3, MoreArgs = list(z = 1),
                       SIMPLIFY = TRUE, BPPARAM=param)
    checkIdentical(c(3,5,7), result)
    checkIdentical(n_connections, .n_connections())
}


test_BatchtoolsParam_bpvec <- function() {
    ## Mutlticore
    param <- BatchtoolsParam(workers=2)
    result <- bpvec(1:10, seq_along, BPPARAM=param)
    target <- as.integer(rep(1:5, 2))
    checkIdentical(target, result)

    ## socket
    param <- BatchtoolsParam(workers=2, cluster="socket")
    result <- bpvec(1:10, seq_along, BPPARAM=param)
    target <- as.integer(rep(1:5,2))
    checkIdentical(target, result)
}


test_BatchtoolsParam_bpvectorize <- function() {
    psqrt <- bpvectorize(sqrt)
    checkTrue(is(psqrt, "function"))
    ## Mutlticore
    param <- BatchtoolsParam(workers=2)
    bpseq_along <- bpvectorize(seq_along, BPPARAM=param)

    res <- bpseq_along(1:10)
    target <- as.integer(rep(1:5, 2))
    checkIdentical(as.integer(target), res)

    ## Socket
    param <- BatchtoolsParam(workers=2, cluster="socket")
    bpseq_along <- bpvectorize(seq_along, BPPARAM=param)

    res <- bpseq_along(1:10)
    target <- as.integer(rep(1:5, 2))
    checkIdentical(as.integer(target), res)
}


test_BatchtoolsParam_bpiterate <- function() {
    n_connections <- .n_connections()
    ## Iterator function
    ITER <- function(n=5) {
        i <- 0L
        function() {
            i <<- i + 1L
            if (i > n)
                return(NULL)
        rep(i, 100)
        }
    }

    ## test function
    FUN <- function(x, k) {
        sum(x) + k
    }

    ## Multicore cluster
    param <- BatchtoolsParam()
    res <- bpiterate(ITER=ITER(), FUN=FUN, k=5, BPPARAM=param)
    ## Check Identical result
    target <- list(105, 205, 305, 405, 505)
    checkIdentical(target, res)

    ## socket cluster
    param <- BatchtoolsParam(cluster="socket")
    res <- bpiterate(ITER=ITER(), FUN=FUN, k=5, BPPARAM=param)
    ## Check Identical result
    checkIdentical(target, res)

    ## Test REDUCE on socket
    res <- bpiterate(ITER=ITER(), FUN=FUN, k=5,
                     REDUCE=`+`,
                     BPPARAM=param)
    ## Check Identical result
    checkIdentical(1525, res)

    ## Test REDUCE, init on mutlicore
    param <- BatchtoolsParam()
    res <- bpiterate(ITER=ITER(), FUN=FUN, k=5,
                     REDUCE=`+`, init = 10,
                     BPPARAM=param)
    ## Check Identical result
    checkIdentical(1535, res)
    checkIdentical(n_connections, .n_connections())
}


test_BatchtoolsParam_bpsaveregistry <- function() {
    .bpregistryargs <- BiocParallel:::.bpregistryargs
    .bpsaveregistry <- BiocParallel:::.bpsaveregistry
    .bpsaveregistry_path <- BiocParallel:::.bpsaveregistry_path
    file.dir <- tempfile()

    ## Set param with save registry
    registryargs <- batchtoolsRegistryargs(file.dir = file.dir)
    param <- BatchtoolsParam(saveregistry=TRUE, registryargs = registryargs)

    checkIdentical(.bpsaveregistry(param), TRUE)
    checkIdentical(.bpregistryargs(param)$file.dir, file.dir)

    ## increment path extension
    file.dir <- file.path(dirname(file.dir), basename(file.dir))
    checkIdentical(.bpsaveregistry_path(param), paste0(file.dir, "-1"))
    dir.create(.bpsaveregistry_path(param))
    checkIdentical(.bpsaveregistry_path(param), paste0(file.dir, "-2"))

    ## create registry
    path <- .bpsaveregistry_path(param)
    checkTrue(!dir.exists(path))
    res <- bplapply(1:5, sqrt, BPPARAM=param)
    checkTrue(dir.exists(path))
}
