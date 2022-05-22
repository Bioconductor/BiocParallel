message("Testing errorhandling")

## NOTE: On Windows, MulticoreParam() throws a warning and instantiates
##       a single FORK worker using scripts from parallel. No logging or
##       error catching is implemented.

checkExceptionText <- function(expr, txt, negate=FALSE, msg="")
{
    x <- try(eval(expr), silent=TRUE)
    checkTrue(inherits(x, "condition"), msg=msg)
    checkTrue(xor(negate, grepl(txt, as.character(x), fixed=TRUE)), msg=msg)
}

test_composeTry <- function() {
    .composeTry <- BiocParallel:::.composeTry
    .workerOptions <- BiocParallel:::.workerOptions
    .error_unevaluated <- BiocParallel:::.error_unevaluated
    X <- as.list(1:6); X[[2]] <- "2"; X[[6]] <- -1

    ## Evaluate all jobs regardless of errors
    ## e.g., SerialParam(stop.on.error=FALSE)
    OPTIONS <- .workerOptions(stop.on.error = FALSE)
    tsqrt <- .composeTry(sqrt, OPTIONS, NULL)
    current <- tryCatch(suppressWarnings(lapply(X, tsqrt)), error=identity)
    target <- list(length(X))
    for (i in seq_along(X))
        target[[i]] <- tryCatch(suppressWarnings(sqrt(X[[i]])), error=identity)
    tok <- !vapply(target, is, logical(1), "error")
    checkIdentical(tok, bpok(current))
    checkIdentical(conditionMessage(target[[which(!tok)]]),
                   conditionMessage(current[[which(!bpok(current))]]))
    checkIdentical(target[tok], current[bpok(current)])

    ## stop evaluation when error occurs; entire vector returned with
    ## 'unevaluated' components. e.g., SnowParam(stop.on.error=TRUE)
    OPTIONS <- .workerOptions(stop.on.error = TRUE)
    tsqrt <- .composeTry(sqrt, OPTIONS, NULL)
    current <- lapply(X, tsqrt)
    checkTrue(is(current[[2]], "remote_error"))
    checkTrue(all(vapply(current[-(1:2)], is, logical(1), "unevaluated_error")))

    ## illogical
    checkException(.composeTry(sqrt, FALSE, FALSE, TRUE, timeout=20L),
                   silent=TRUE)
}

test_SerialParam_stop.on.error <- function()
{
    X <- list(1, "2", 3)

    ## stop.on.error=TRUE; lapply-like
    p <- SerialParam()
    checkIdentical(TRUE, bpstopOnError(p))
    checkException(bplapply(X, sqrt, BPPARAM=p), silent=TRUE)
    current <- tryCatch(bplapply(X, sqrt, BPPARAM=p), error=identity)
    checkTrue(is(current, "bplist_error"))
    target <- "BiocParallel errors\n  1 remote errors, element index: 2\n  1 unevaluated and other errors\n  first remote error:\nError in FUN(...): non-numeric argument to mathematical function\n"
    checkIdentical(target, conditionMessage(current))
    target <- tryCatch(lapply(X, sqrt), error=identity)
    checkIdentical(
        conditionMessage(target),
        conditionMessage(bpresult(current)[[2]])
    )

    result <- bptry(bplapply(X, sqrt, BPPARAM=p)) # issue #142
    checkIdentical(c(TRUE, FALSE, FALSE), bpok(result))

    ## stop.on.error=FALSE
    p <- SerialParam(stop.on.error=FALSE) #
    checkException(bplapply(X, sqrt, BPPARAM=p), silent=TRUE)
    current <- tryCatch(bplapply(X, sqrt, BPPARAM=p), error=identity)
    checkTrue(is(current, "bplist_error"))
    result <- bpresult(current)
    checkIdentical(c(TRUE, FALSE, TRUE), bpok(result))
    checkTrue(is(result[[2]], "remote_error"))
    checkIdentical(list(sqrt(1), sqrt(3)), result[bpok(result)])

    result <- bptry(bplapply(X, sqrt, BPPARAM=p))
    checkIdentical(c(TRUE, FALSE, TRUE), bpok(result))
}

test_stop.on.error <- function() {
    checkException(bplapply("2", sqrt), silent=TRUE)
    checkException(bplapply(c(1, "2"), sqrt), silent=TRUE)
    checkException(bplapply(c(1, "2", 3), sqrt), silent=TRUE)

    cls <- tryCatch(bplapply(c(1, "2", 3), sqrt), error=class)
    checkIdentical(c("bplist_error", "bperror", "error", "condition"), cls)
}

test_catching_errors <- function()
{
    x <- 1:10
    y <- rev(x)
    f <- function(x, y) if (x > y) stop("whooops") else x + y

    cl <- parallel::makeCluster(2)
    doParallel::registerDoParallel(cl)
    params <- list(
               snow=SnowParam(2, stop.on.error = FALSE),
               dopar=DoparParam(stop.on.error = FALSE),
               batchjobs=BatchJobsParam(2, progressbar=FALSE,stop.on.error = FALSE)
               )
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2, stop.on.error = FALSE)

    for (param in params) {
        res <- tryCatch({
            bplapply(list(1, "2", 3), sqrt, BPPARAM=param)
        }, error=identity)
        checkTrue(is(res, "bplist_error"))
        result <- bpresult(res)
        checkTrue(length(result) == 3L)
        msg <- "non-numeric argument to mathematical function"
        checkIdentical(conditionMessage(result[[2]]), msg)
    }

    ## clean up
    foreach::registerDoSEQ()
    parallel::stopCluster(cl)
    closeAllConnections()
}

test_BPREDO <- function()
{
    f = sqrt
    x = list(1, "2", 3)
    x.fix = list(1, 2, 3)

    cl <- parallel::makeCluster(2)
    doParallel::registerDoParallel(cl)
    params <- list(
               snow=SnowParam(2, stop.on.error = FALSE),
               dopar=DoparParam(stop.on.error = FALSE),
               batchjobs=BatchJobsParam(2, progressbar=FALSE,stop.on.error = FALSE)
               )
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2, stop.on.error = FALSE)

    for (param in params) {
        res <- tryCatch({
            bplapply(x, f, BPPARAM=param)
        }, error=identity)
        checkTrue(is(res, "bplist_error"))
        result <- bpresult(res)
        checkIdentical(3L, length(result))
        checkTrue(inherits(result[[2]], "remote_error"))

        ## data not fixed
        res2 <- tryCatch({
            bplapply(x, f, BPPARAM=param, BPREDO=res)
        }, error=identity)
        checkTrue(is(res2, "bplist_error"))
        result <- bpresult(res2)
        checkIdentical(3L, length(result))
        checkTrue(is(result[[2]], "remote_error"))
        checkIdentical(as.list(sqrt(c(1, 3))), result[c(1, 3)])

        ## data fixed
        res3 <- bplapply(x.fix, f, BPPARAM=param, BPREDO=res2)
        checkIdentical(as.list(sqrt(1:3)), res3)
    }

    ## clean up
    foreach::registerDoSEQ()
    parallel::stopCluster(cl)
    closeAllConnections()
}

test_bpvec_BPREDO <- function()
{
    f = function(i) if (6 %in% i) stop() else sqrt(i)
    x = 1:10

    cl <- parallel::makeCluster(2)
    doParallel::registerDoParallel(cl)
    params <- list(
               snow=SnowParam(2, stop.on.error = FALSE),
               dopar=DoparParam(stop.on.error = FALSE),
               batchjobs=BatchJobsParam(2, progressbar=FALSE,stop.on.error = FALSE)
               )
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2, stop.on.error = FALSE)

    for (param in params) {
        res <- bptry(bpvec(x, f, BPPARAM=param), bplist_error=identity)
        checkTrue(is(res, "bplist_error"))
        result <- bpresult(res)
        checkIdentical(2L, length(result))
        checkTrue(inherits(result[[2]], "condition"))

        ## data not fixed
        res2 <- bptry(bpvec(x, f, BPPARAM=param, BPREDO=res),
                      bplist_error=identity)
        checkTrue(is(res2, "bplist_error"))
        result <- bpresult(res2)
        checkIdentical(2L, length(result))
        checkTrue(is(result[[2]], "remote_error"))

        ## data fixed
        res3 <- bpvec(x, sqrt, BPPARAM=param, BPREDO=res2)
        checkIdentical(sqrt(x), res3)
    }

    ## clean up
    foreach::registerDoSEQ()
    parallel::stopCluster(cl)
    closeAllConnections()
}

test_bpiterate_BPREDO <- function()
{
    n <- 100L
    ntask <- n
    iter_factory <- function(n){
        i <- 0L
        function() if(i<n) i <<- i + 1
    }

    FUN <- function(x) {
        if (x %in% 2:3)
            0L
        else
            x
    }

    FUN1 <- function(x) {
        if (x %in% 2:3)
            stop("hit error")
        else x
    }

    stop.on.error <- TRUE
    params <- list(
        serial=SerialParam(stop.on.error=stop.on.error),
        snow=SnowParam(2, stop.on.error=stop.on.error))
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2, stop.on.error=stop.on.error)

    for (param in params) {
        bptasks(param) <- ntask
        res0 <- bpiterate(iter_factory(n), FUN,BPPARAM = param)
        checkException(bpiterate(iter_factory(n), FUN1, BPPARAM = param))
        res1 <- bptry(bpiterate(iter_factory(n), FUN1, BPPARAM = param))
        res2 <- bpiterate(iter_factory(n), FUN, BPREDO = res1, BPPARAM = param)

        checkIdentical(res0, res2)
    }

    stop.on.error <- FALSE
    params <- list(
        serial=SerialParam(stop.on.error=stop.on.error),
        snow=SnowParam(2, stop.on.error=stop.on.error))
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2, stop.on.error=stop.on.error)

    for (param in params) {
        bptasks(param) <- ntask
        res0 <- bpiterate(iter_factory(n), FUN,BPPARAM = param)
        checkException(bpiterate(iter_factory(n), FUN1, BPPARAM = param))
        res1 <- bptry(bpiterate(iter_factory(n), FUN1, BPPARAM = param))
        res2 <- bpiterate(iter_factory(n), FUN, BPREDO = res1, BPPARAM = param)

        checkEquals(length(res0), length(res1))
        checkIdentical(res0, res2)
    }

}
