## Test 'stop.on.error', 'catch.errors'. See test_logging.R
## for tests with 'log' and 'progressbar'.

# FIXME we need the windows workaround
library(doParallel)
registerDoParallel()

checkExceptionText <- function(expr, txt, negate=FALSE, msg="")
{
    x <- try(eval(expr), silent=TRUE)
    checkTrue(inherits(x, "try-error"), msg=msg)
    checkTrue(xor(negate, grepl(txt, as.character(x), fixed=TRUE)), msg=msg)
}

test_catch.errors <- function()
{
    x <- 1:10
    y <- rev(x)
    f <- function(x, y) if (x > y) stop("whooops") else x + y

    ## catch.errors = FALSE
    params <- list(
        serial=SerialParam(catch.errors=FALSE),
        snow=SnowParam(catch.errors=FALSE),
        mc=MulticoreParam(catch.errors=FALSE),
        dopar=DoparParam(catch.errors=FALSE),
        batchjobs=BatchJobsParam(catch.errors=FALSE, progressbar=FALSE))

    for (param in params) {
        checkExceptionText(bpmapply(f, x, y, BPPARAM=param),
            "Error in FUN(...): whooops", negate=TRUE)
    }

    ## catch.errors = TRUE 
    params <- list(
        serial=SerialParam(),
        snow=SnowParam(),
        mc=MulticoreParam(),
        dopar=DoparParam(),
        batchjobs=BatchJobsParam(progressbar=FALSE))


    for (param in params) {
        res <- bplapply(list(1, "2", 3), sqrt, BPPARAM=param)
        checkTrue(length(res) == 3L)
        msg <- "non-numeric argument to mathematical function"
        checkIdentical(conditionMessage(res[[2]]), msg)
    }

    closeAllConnections()
    TRUE
}

test_BPREDO <- function()
{
    f = sqrt
    x = list(1, "2", 3) 
    x.fix = list(1, 2, 3) 

    params <- list(
        serial=SerialParam(),
        snow=SnowParam(),
        mc=MulticoreParam(),
        dopar=DoparParam(),
        batchjobs=BatchJobsParam(progressbar=FALSE))

    for (param in params) {
        res <- bpmapply(f, x, BPPARAM=param, SIMPLIFY=TRUE)
        checkTrue(inherits(res[[2]], "condition"))

        ## data not fixed
        res2 <- bpmapply(f, x, BPPARAM=param, BPREDO=res, SIMPLIFY=TRUE)
        checkTrue(inherits(res2[[2]], "condition"))

        ## data fixed
        res3 <- bpmapply(f, x.fix, BPPARAM=param, BPREDO=res, SIMPLIFY=TRUE)
        checkIdentical(res3, sqrt(1:3))
    }

    closeAllConnections()
    TRUE
}
