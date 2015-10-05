## NOTE: On Windows, MulticoreParam() throws a warning and instantiates
##       a single FORK worker using scripts from parallel. No logging or 
##       error catching is implemented.

library(doParallel)

checkExceptionText <- function(expr, txt, negate=FALSE, msg="")
{
    x <- try(eval(expr), silent=TRUE)
    checkTrue(inherits(x, "condition"), msg=msg)
    checkTrue(xor(negate, grepl(txt, as.character(x), fixed=TRUE)), msg=msg)
}

test_catching_errors <- function()
{
    if (.Platform$OS.type != "windows") {
        x <- 1:10
        y <- rev(x)
        f <- function(x, y) if (x > y) stop("whooops") else x + y

        registerDoParallel(2)
        params <- list(
            serial=SerialParam(catch.errors=TRUE),
            snow=SnowParam(2),
            dopar=DoparParam(),
            batchjobs=BatchJobsParam(progressbar=FALSE),
            mc <- MulticoreParam(2))

        for (param in params) {
            res <- bplapply(list(1, "2", 3), sqrt, BPPARAM=param)
            checkTrue(length(res) == 3L)
            msg <- "non-numeric argument to mathematical function"
            checkIdentical(conditionMessage(res[[2]]), msg)
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

        registerDoParallel(2)
        params <- list(
            serial=SerialParam(catch.errors=TRUE),
            snow=SnowParam(2),
            dopar=DoparParam(),
            batchjobs=BatchJobsParam(progressbar=FALSE),
            mc <- MulticoreParam(2))

        for (param in params) {
            res <- bpmapply(f, x, BPPARAM=param, SIMPLIFY=TRUE)
            checkTrue(inherits(res[[2]], "condition"))
            closeAllConnections()
            Sys.sleep(0.25)

            ## data not fixed
            res2 <- bpmapply(f, x, BPPARAM=param, BPREDO=res, SIMPLIFY=TRUE)
            checkTrue(inherits(res2[[2]], "condition"))
            closeAllConnections()
            Sys.sleep(0.25)

            ## data fixed
            res3 <- bpmapply(f, x.fix, BPPARAM=param, BPREDO=res, SIMPLIFY=TRUE)
            checkIdentical(res3, sqrt(1:3))
            closeAllConnections()
            Sys.sleep(0.25)
        }

        ## clean up
        env <- foreach:::.foreachGlobals
        rm(list=ls(name=env), pos=env)
        closeAllConnections()
        TRUE
    } else TRUE
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
    params <- list(snow=SnowParam(2, stop.on.error=TRUE))
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2, stop.on.error=TRUE)

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
