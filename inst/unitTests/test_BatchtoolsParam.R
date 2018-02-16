test_BatchtoolsParam_constructor <- function() {
    result <- BatchtoolsParam()
    checkTrue(validObject(result))

    isWindows <- .Platform$OS.type == "windows"
    clusterType <- if (isWindows) "socket" else "multicore"
    checkIdentical(clusterType, bpbackend(result))
    expected <- BiocParallel:::.snowCores(isWindows)
    checkIdentical(expected, bpnworkers(BatchtoolsParam()))
}

test_batchtoolsWorkers <- function() {
    isWindows <- .Platform$OS.type == "windows"
    expected <- BiocParallel:::.snowCores(isWindows)
    checkIdentical(expected, batchtoolsWorkers())
}
