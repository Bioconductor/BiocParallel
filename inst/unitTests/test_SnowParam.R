test_SnowParam_construction <- function() 
{
    checkException(SnowParam(logdir = tempdir())) 

    p <- MulticoreParam(jobname = 'test')
    checkIdentical(bpjobname(p), 'test')
}

test_SnowParam_SOCK <- function() 
{
    if (!requireNamespace("snow", quietly=TRUE))
        DEACTIVATED("'snow' package did not load")

    param <- SnowParam(2, "SOCK", tasks=2)
    checkIdentical(FALSE, bpisup(param))

    exp <- bplapply(1:2, function(i) Sys.getpid(), BPPARAM=param)
    checkIdentical(2L, length(unique(unlist(exp))))
    checkIdentical(FALSE, bpisup(param))
}

test_SnowParam_SOCK_character <- function()
{
    bpstop(bpstart(SnowParam("localhost")))
}

test_SnowParam_MPI <- function() 
{
    if (.Platform$OS.type == "windows")
        DEACTIVATED("MPI tests not run on Windows")

    DEACTIVATED("MPI tests not run")

    param <- SnowParam(2, "MPI", tasks=2)
    checkIdentical(FALSE, bpisup(param))

    exp <- bplapply(1:2, function(i) mpi.comm.rank(), BPPARAM=param)
    checkIdentical(c(1L, 2L), sort(unlist(exp)))
    checkIdentical(FALSE, bpisup(param))
}

test_SnowParam_coerce_from_SOCK <- function()
{
    if (!requireNamespace("snow", quietly=TRUE))
        DEACTIVATED("'snow' package did not load")

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
        DEACTIVATED("MPI tests not run on Windows")
    
    if (!requireNamespace("snow", quietly=TRUE) ||
         !requireNamespace("Rmpi", quietly=TRUE))
        DEACTIVATED("'snow' and/or 'Rmpi' package did not load")

    DEACTIVATED("MPI tests not run")

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

    if (!requireNamespace("snow", quietly=TRUE) ||
         !requireNamespace("Rmpi", quietly=TRUE))
        DEACTIVATED("'snow' and/or 'Rmpi' package did not load")

    checkException(SnowParam("host", "MPI"), silent=TRUE)
    checkException(SnowParam("host", "FORK"), silent=TRUE)
}
