message("Testing SnowParam")

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

    obs <- tryCatch(bpstop(p), warning=conditionMessage)
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

test_SnowParam_progressbar <- function()
{
    checkIdentical(bptasks(SnowParam()), 0L)
    checkIdentical(bptasks(SnowParam(tasks = 0L, progressbar = TRUE)), 0L)
    checkIdentical(
        bptasks(SnowParam(progressbar = TRUE)),
        BiocParallel:::TASKS_MAXIMUM
    )
}

test_SnowParam_bpforceGC <- function() {
    checkIdentical(FALSE, bpforceGC(SnowParam()))
    checkIdentical(FALSE, bpforceGC(SnowParam(force.GC = FALSE)))
    checkIdentical(TRUE, bpforceGC(SnowParam(force.GC = TRUE)))
    checkException(SnowParam(force.GC = NA), silent = TRUE)
    checkException(SnowParam(force.GC = 1:2), silent = TRUE)
}

.test_cache <- function(msg) {
    ## No initial cache
    msg1 <- BiocParallel:::.load_task_static(msg)
    checkIdentical(msg, msg1)

    ## Extract the dynamic part of the task
    msg2 <- BiocParallel:::.task_dynamic(msg)
    checkTrue(!xor(msg2$static.fun, isTRUE(msg2$data$fun)))
    if (length(msg2$static.args))
        checkTrue(!any(msg2$static.args %in% names(msg2$data$args)))
    else
        checkTrue(all(msg2$static.args %in% names(msg2$data$args)))

    ## rebuild the EXEC
    msg3 <- BiocParallel:::.load_task_static(msg2)
    checkIdentical(msg, msg3)

    ## create another different msg
    msg4 <- BiocParallel:::.EXEC("test", function(x)x,
                                 args = list(a=1,b=1,c=1),
                                 static.fun = TRUE,
                                 static.args = c("a","b")
    )
    msg5 <- BiocParallel:::.load_task_static(msg4)
    checkIdentical(msg5, msg4)
}

test_SnowParam_taskCache <- function() {
    msg1 <- BiocParallel:::.EXEC("mytag1", identity,
                                 args = list(a=1,b=2,c=3)
    )
    .test_cache(msg1)

    msg2 <- BiocParallel:::.EXEC("mytag2", identity,
                                 args = list(a=1,b=2,c=3),
                                 static.fun = TRUE
    )
    .test_cache(msg2)

    msg3 <- BiocParallel:::.EXEC("mytag3", identity,
                                 args = list(a=1,b=2,c=3),
                                 static.fun = TRUE,
                                 static.args = c("a","b")
    )
    .test_cache(msg3)

    msg4 <- BiocParallel:::.EXEC("mytag", identity,
                                 args = NULL,
                                 static.fun = TRUE
    )
    .test_cache(msg4)
}


test_SnowParam_fallback <- function(){
    ## trigger fallback
    p <- SnowParam(1)
    res <- bplapply(1, function(x) Sys.getpid(), BPPARAM = p)[[1]]
    checkTrue(res == Sys.getpid())

    ## disable fallback
    bpfallback(p) <- FALSE
    res <- bplapply(1, function(x) Sys.getpid(), BPPARAM = p)[[1]]
    checkTrue(res != Sys.getpid())

    ## enable fallback again
    bpfallback(p) <- TRUE
    res <- bplapply(1, function(x) Sys.getpid(), BPPARAM = p)[[1]]
    checkTrue(res == Sys.getpid())

    ## no fallback
    p <- SnowParam(2)
    res <- bplapply(1, function(x) Sys.getpid(), BPPARAM = p)[[1]]
    checkTrue(res != Sys.getpid())
}
