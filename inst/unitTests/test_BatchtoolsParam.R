test_BatchtoolsParam_constructor <- function() {
    ## TODO: is(param$registry, "NULLRegistry")
    param <- BatchtoolsParam()
    checkTrue(validObject(param))

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

test_BatchtoolsParam_bpisup_start_stop <- function() {
    checkIdentical(FALSE, bpisup(BatchtoolsParam()))
    checkIdentical(TRUE, bpisup(bpstart(BatchtoolsParam())))
    ## TODO: other cluster types
    ## TODO: is an interactive cluster always up? yes
    ## TODO bpstop
}
