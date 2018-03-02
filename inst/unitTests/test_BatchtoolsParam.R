test_BatchtoolsParam_constructor <- function() {
    param <- BatchtoolsParam()
    checkTrue(validObject(param))

    checkTrue(is(param$registry, "NULLRegistry"))

    isWindows <- .Platform$OS.type == "windows"
    nworkers <- BiocParallel:::.snowCores(isWindows)

    cluster <- if (isWindows) "socket" else "multicore"
    checkIdentical(cluster, bpbackend(param))
    checkIdentical(nworkers, bpnworkers(param))

    checkIdentical(3L, bpnworkers(BatchtoolsParam(3L)))

    cluster <- "socket"
    param <- BatchtoolsParam(cluster=cluster)
    checkIdentical(cluster, bpbackend(param))
    checkIdentical(nworkers, bpnworkers(param))

    cluster <- "multicore"
    param <- BatchtoolsParam(cluster=cluster)
    checkIdentical(cluster, bpbackend(param))
    checkIdentical(nworkers, bpnworkers(param))

    cluster <- "interactive"
    param <- BatchtoolsParam(cluster=cluster)
    checkIdentical(cluster, bpbackend(param))
    checkIdentical(1L, bpnworkers(param))

    cluster <- "unknown"
    checkException(BatchtoolsParam(cluster=cluster))
}

test_batchtoolsWorkers <- function() {
    isWindows <- .Platform$OS.type == "windows"
    expected <- BiocParallel:::.snowCores(isWindows)

    checkIdentical(expected, batchtoolsWorkers())
    checkIdentical(expected, batchtoolsWorkers("socket"))
    if (!isWindows)
        checkIdentical(expected, batchtoolsWorkers("multicore"))

    checkIdentical(1L, batchtoolsWorkers("interactive"))

    checkException(batchtoolsWorkers("unknown"))
}

test_BatchtoolsParam_bpisup_start_stop_default<- function() {
    param <- BatchtoolsParam(workers=2)
    checkIdentical(FALSE, bpisup(param))
    checkIdentical(TRUE, bpisup(bpstart(param)))
    checkIdentical(FALSE, bpisup(bpstop(param)))
}

## TODO: other cluster types
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

    ## Check for multicore
    cluster <- "multicore"
    param <- BatchtoolsParam(workers=2, cluster=cluster)
    result <- bplapply(1:5, fun, BPPARAM=param)
    checkIdentical(2L, length(unique(unlist(result))))

    ## Check for socket
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
    on.exit(bpstop(param))
    checkTrue(!is(param$registry, "NULLRegistry"))
    checkTrue(is(param$registry, "Registry"))
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
