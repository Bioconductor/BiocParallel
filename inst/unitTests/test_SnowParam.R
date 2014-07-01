test_SnowParam_PSOCK <- function() {
    param <- SnowParam(2, "PSOCK")
    checkIdentical(FALSE, bpisup(param))

    exp <- bplapply(1:2, function(i) Sys.getpid(), BPPARAM=param)
    checkIdentical(2L, length(unique(unlist(exp))))
    checkIdentical(FALSE, bpisup(param))
}

test_SnowParam_SOCK <- function() {
    if (!suppressWarnings(require(snow)))
        ## quietly succeed if 'snow' not available
        return()
    param <- SnowParam(2, "SOCK")
    checkIdentical(FALSE, bpisup(param))

    exp <- bplapply(1:2, function(i) Sys.getpid(), BPPARAM=param)
    checkIdentical(2L, length(unique(unlist(exp))))
    checkIdentical(FALSE, bpisup(param))
}

test_SnowParam_MPI <- function() {
    if (.Platform$OS.type == "windows")
        return()
    if (!(suppressWarnings(require(snow)) ||
          suppressWarnings(require(Rmpi))))
        ## quietly succeed if 'snow', 'Rmpi' not available
        return()
    param <- SnowParam(2, "MPI")
    checkIdentical(FALSE, bpisup(param))

    exp <- bplapply(1:2, function(i) mpi.comm.rank(), BPPARAM=param)
    checkIdentical(c(1L, 2L), sort(unlist(exp)))
    checkIdentical(FALSE, bpisup(param))
}

test_SnowParam_coerce_from_PSOCK <- function()
{
    cl <- parallel::makeCluster(2L)
    p <- as(cl, "SnowParam")
    checkTrue(validObject(p))

    obs <- tryCatch(bpstart(p), error=conditionMessage)
    exp <- "'bpstart' not available; instance from outside BiocParallel?"
    checkIdentical(exp, obs)

    obs <- tryCatch(bpstop(p), error=conditionMessage)
    exp <- "'bpstop' not available; instance from outside BiocParallel?"
    checkIdentical(exp, obs)

    exp <- bplapply(1:2, function(i) Sys.getpid(), BPPARAM=p)
    checkIdentical(2L, length(unique(unlist(exp))))
    checkIdentical(TRUE, bpisup(p))

    parallel::stopCluster(cl)
}

test_SnowParam_coerce_from_SOCK <- function()
{
    if (!suppressWarnings(require(snow)))
        ## quietly succeed if 'snow' not available
        return()

    cl <- parallel::makeCluster(2L, "SOCK")
    p <- as(cl, "SnowParam")
    checkTrue(validObject(p))

    obs <- tryCatch(bpstart(p), error=conditionMessage)
    exp <- "'bpstart' not available; instance from outside BiocParallel?"
    checkIdentical(exp, obs)

    obs <- tryCatch(bpstop(p), error=conditionMessage)
    exp <- "'bpstop' not available; instance from outside BiocParallel?"
    checkIdentical(exp, obs)

    exp <- bplapply(1:2, function(i) Sys.getpid(), BPPARAM=p)
    checkIdentical(2L, length(unique(unlist(exp))))
    checkIdentical(TRUE, bpisup(p))

    parallel::stopCluster(cl)
}

test_SnowParam_coerce_from_MPI <- function()
{
    if (.Platform$OS.type == "windows")
        return()
    if (!(suppressWarnings(require(snow)) ||
          suppressWarnings(require(Rmpi))))
        ## quietly succeed if 'snow', 'Rmpi' not available
        return()

    cl <- parallel::makeCluster(2L, "MPI")
    p <- as(cl, "SnowParam")
    checkTrue(validObject(p))

    obs <- tryCatch(bpstart(p), error=conditionMessage)
    exp <- "'bpstart' not available; instance from outside BiocParallel?"
    checkIdentical(exp, obs)

    obs <- tryCatch(bpstop(p), error=conditionMessage)
    exp <- "'bpstop' not available; instance from outside BiocParallel?"
    checkIdentical(exp, obs)

    exp <- bplapply(1:2, function(i) mpi.comm.rank(), BPPARAM=p)
    checkIdentical(c(1L, 2L), sort(unlist(exp)))
    checkIdentical(TRUE, bpisup(p))

    parallel::stopCluster(cl)
}

test_SnowParam_workers <- function()
{
    if (.Platform$OS.type == "windows")
        return()
    if (!(suppressWarnings(require(snow)) ||
          suppressWarnings(require(Rmpi))))
        ## quietly succeed if 'snow', 'Rmpi' not available
        return()

    checkException(SnowParam("host", "MPI"), silent=TRUE)
    checkException(SnowParam("host", "FORK"), silent=TRUE)
}
