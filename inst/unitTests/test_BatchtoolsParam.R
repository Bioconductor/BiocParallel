test_BatchtoolsParam_constructor <- function() {
    param <- BatchtoolsParam()
    checkTrue(validObject(param))

    checkTrue(is(param$registry, "NULLRegistry"))

    isWindows <- .Platform$OS.type == "windows"
    nworkers <- BiocParallel:::.snowCores()
    cluster <- if (isWindows) "socket" else "multicore"
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

test_batchtoolsWorkers <- function() {
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

test_BatchtoolsParam_bpisup_start_stop_default<- function() {
    param <- BatchtoolsParam(workers=2)
    checkIdentical(FALSE, bpisup(param))
    checkIdentical(TRUE, bpisup(bpstart(param)))
    checkIdentical(FALSE, bpisup(bpstop(param)))
}

test_BatchtoolsParam_bpisup_start_stop_socket <- function() {
    cluster <- "socket"
    param <- BatchtoolsParam(workers=2, cluster=cluster)
    checkIdentical(cluster, bpbackend(param))

    checkIdentical(FALSE, bpisup(param))
    checkIdentical(TRUE, bpisup(bpstart(param)))
    checkIdentical(FALSE, bpisup(bpstop(param)))
}

test_BatchtoolsParam_bpisup_start_stop_interactive <- function() {
    cluster <- "interactive"
    param <- BatchtoolsParam(workers=2, cluster=cluster)
    checkIdentical(cluster, bpbackend(param))

    checkIdentical(FALSE, bpisup(param))
    checkIdentical(TRUE,bpisup( bpstart(param)))
    checkIdentical(FALSE, bpisup(bpstop(param)))
}

test_BatchtoolsParam_bplapply <- function() {
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
}

## Check registry
test_BatchtoolsParam_registry <- function() {
    param <- BatchtoolsParam()
    checkTrue(is(param$registry, "NULLRegistry"))
    bpstart(param)
    checkTrue(!is(param$registry, "NULLRegistry"))
    checkTrue(is(param$registry, "Registry"))
    bpstop(param)
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
    checkEquals(30L * 24L * 60L * 60L, bptimeout(BatchtoolsParam()))
    checkEquals(123L, bptimeout(BatchtoolsParam(timeout=123)))
}

## Check bpRNGseed
test_BatchtoolsParam_bpRNGseed <- function() {
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
}

test_BatchtoolsParam_bplog <- function() {
    ## Test param w/o log and logdir
    checkTrue(is.na(bplogdir(BatchtoolsParam())))
    checkTrue(!bplog(BatchtoolsParam()))
    ## test param with log, w/o logdir
    param <- BatchtoolsParam(log=TRUE)
    checkTrue(bplog(param))
    checkTrue(is.na(bplogdir(param)))
    ## Check if setter works
    temp_log_dir <- tempfile()
    bplogdir(param) <- temp_log_dir
    checkIdentical(temp_log_dir, bplogdir(param))
    ## test param without log and w logdir
    checkException(BatchtoolsParam(logdir=temp_log_dir))
    ## check logs in logdir
    param <- BatchtoolsParam(log=TRUE, logdir=temp_log_dir)
    bplapply(1:5, sqrt, BPPARAM=param)
    checkTrue(file.exists(temp_log_dir))
    checkTrue(file.exists(file.path(temp_log_dir, "logs")))
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
}

## TODO: write tests for other cluster types, slurm, lsf, torque, openlava
