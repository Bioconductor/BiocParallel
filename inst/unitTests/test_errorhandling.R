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
    X <- as.list(1:6); X[[2]] <- "2"; X[[6]] <- -1

    ## fail hard, e.g., SerialParam()
    tsqrt <- .composeTry(sqrt, FALSE, TRUE, TRUE, timeout=20L)
    current <- tryCatch(lapply(X, tsqrt), error=identity)
    target <- tryCatch(lapply(X, sqrt), error=identity)
    checkIdentical(conditionMessage(target), conditionMessage(current))

    ## fail soft, e.g., SerialParam(stop.on.error=FALSE)
    tsqrt <- .composeTry(sqrt, FALSE, FALSE, FALSE, timeout=20L)
    current <- tryCatch(suppressWarnings(lapply(X, tsqrt)), error=identity)
    target <- list(length(X))
    for (i in seq_along(X))
        target[[i]] <- tryCatch(suppressWarnings(sqrt(X[[i]])), error=identity)
    tok <- !vapply(target, is, logical(1), "error")
    checkIdentical(tok, bpok(current))
    checkIdentical(conditionMessage(target[[which(!tok)]]),
                   conditionMessage(current[[which(!bpok(current))]]))
    checkIdentical(target[tok], current[bpok(current)])

    ## fail soft on an individual worker; entire vector returned with
    ## 'unevaluated' components. e.g., SnowParam(stop.on.error=TRUE)
    tsqrt <- .composeTry(sqrt, FALSE, TRUE, FALSE, timeout=20L)
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
    checkTrue(is(current, "remote_error"))
    target <- tryCatch(lapply(X, sqrt), error=identity)
    checkIdentical(conditionMessage(target), conditionMessage(current))

    ## stop.on.error=FALSE
    p <- SerialParam(stop.on.error=FALSE) #
    checkException(bplapply(X, sqrt, BPPARAM=p), silent=TRUE)
    current <- tryCatch(bplapply(X, sqrt, BPPARAM=p), error=identity)
    checkTrue(is(current, "bplist_error"))
    result <- attr(current, "result")
    checkIdentical(c(TRUE, FALSE, TRUE), bpok(result))
    checkTrue(is(result[[2]], "remote_error"))
    checkIdentical(list(sqrt(1), sqrt(3)), result[bpok(result)])
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
    if (.Platform$OS.type != "windows") {
        x <- 1:10
        y <- rev(x)
        f <- function(x, y) if (x > y) stop("whooops") else x + y

        doParallel::registerDoParallel(2)
        params <- list(
            mc = MulticoreParam(2, stop.on.error=FALSE),
            snow=SnowParam(2, stop.on.error=FALSE),
            dopar=DoparParam(stop.on.error=FALSE),
            batchjobs=BatchJobsParam(2, progressbar=FALSE, stop.on.error=FALSE))

        for (param in params) {
            res <- tryCatch({
                bplapply(list(1, "2", 3), sqrt, BPPARAM=param)
            }, error=identity)
            checkTrue(is(res, "bplist_error"))
            result <- attr(res, "result")
            checkTrue(length(result) == 3L)
            msg <- "non-numeric argument to mathematical function"
            checkIdentical(conditionMessage(result[[2]]), msg)
            closeAllConnections()
        }

        ## clean up
        env <- foreach:::.foreachGlobals
        rm(list=ls(name=env), pos=env)
        closeAllConnections()
        TRUE
    } else TRUE
}

test_BPREDO <- function()
{
    if (.Platform$OS.type != "windows") {
        f = sqrt
        x = list(1, "2", 3) 
        x.fix = list(1, 2, 3) 

        doParallel::registerDoParallel(2)
        params <- list(
            mc = MulticoreParam(2, stop.on.error=FALSE),
            snow=SnowParam(2, stop.on.error=FALSE),
            dopar=DoparParam(stop.on.error=FALSE),
            batchjobs=BatchJobsParam(2, progressbar=FALSE, stop.on.error=FALSE))

        for (param in params) {
            res <- tryCatch({
                bplapply(x, f, BPPARAM=param)
            }, error=identity)
            checkTrue(is(res, "bplist_error"))
            result <- attr(res, "result")
            checkIdentical(3L, length(result))
            checkTrue(inherits(result[[2]], "condition"))
            closeAllConnections()
            Sys.sleep(0.25)

            ## data not fixed
            res2 <- tryCatch({
                bplapply(x, f, BPPARAM=param, BPREDO=result)
            }, error=identity)
            checkTrue(is(res2, "bplist_error"))
            result <- attr(res2, "result")
            checkIdentical(3L, length(result))
            checkTrue(is(result[[2]], "remote_error"))
            checkIdentical(as.list(sqrt(c(1, 3))), result[c(1, 3)])
            closeAllConnections()
            Sys.sleep(0.25)

            ## data fixed
            res3 <- bplapply(x.fix, f, BPPARAM=param, BPREDO=result)
            checkIdentical(as.list(sqrt(1:3)), res3)
            closeAllConnections()
            Sys.sleep(0.25)
        }

        ## clean up
        env <- foreach:::.foreachGlobals
        rm(list=ls(name=env), pos=env)
        closeAllConnections()
    }
    TRUE
}

test_bpvec_BPREDO <- function()
{
    if (.Platform$OS.type != "windows") {
        f = function(i) if (2 %in% i) stop() else sqrt(i)
        x = 1:10

        doParallel::registerDoParallel(2)
        params <- list(
            mc = MulticoreParam(2, stop.on.error=FALSE),
            snow=SnowParam(2, stop.on.error=FALSE),
            dopar=DoparParam(stop.on.error=FALSE),
            batchjobs=BatchJobsParam(2, progressbar=FALSE, stop.on.error=FALSE))

        for (param in params) {
            res <- bptry(bpvec(x, f, BPPARAM=param), bplist_error=identity)
            checkTrue(is(res, "bplist_error"))
            result <- attr(res, "result")
            checkIdentical(2L, length(result))
            checkTrue(inherits(result[[1]], "condition"))
            closeAllConnections()
            Sys.sleep(0.25)

            ## data not fixed
            res2 <- bptry(bpvec(x, f, BPPARAM=param, BPREDO=result),
                          bplist_error=identity)
            checkTrue(is(res2, "bplist_error"))
            result <- attr(res2, "result")
            checkIdentical(2L, length(result))
            checkTrue(is(result[[1]], "remote_error"))
            closeAllConnections()
            Sys.sleep(0.25)

            ## data fixed
            res3 <- bpvec(x, sqrt, BPPARAM=param, BPREDO=result)
            checkIdentical(sqrt(x), res3)
            closeAllConnections()
            Sys.sleep(0.25)
        }

        ## clean up
        env <- foreach:::.foreachGlobals
        rm(list=ls(name=env), pos=env)
        closeAllConnections()
    }
    TRUE
}

test_bpiterate_errors <- function()
{
    quiet <- suppressMessages
    .lazyCount <- function(count) {
        count <- count
        i <- 0L
 
        function() {
            if (i >= count)
                return(NULL)
            else
                i <<- i + 1L
 
            if (i == 2)
                "2"
            else
                i
        }
    }

    FUN <- function(count, ...) {
        if (count == 2)
            stop("hit error")
        else count 
    }
    params <- list(snow=SnowParam(2, stop.on.error=FALSE))
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2, stop.on.error=FALSE)

    for (p in params) {
        ITER <- .lazyCount(3)
        quiet(res <- bpiterate(ITER, FUN, BPPARAM=p))
        checkTrue(is(res[[2]], "condition"))
        closeAllConnections()
    }

    ## clean up
    closeAllConnections()
    TRUE
}
