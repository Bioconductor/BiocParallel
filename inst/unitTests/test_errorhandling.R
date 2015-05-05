## Test 'stop.on.error', 'catch.errors'. See test_logging.R
## for tests with 'log' and 'progressbar'.

checkExceptionText <- function(expr, txt, negate=FALSE, msg="")
{
    x <- try(eval(expr), silent=TRUE)
    checkTrue(inherits(x, "try-error"), msg=msg)
    checkTrue(xor(negate, grepl(txt, as.character(x), fixed=TRUE)), msg=msg)
}

test_catch.errors <- function()
{
    # FIXME we need the windows workaround
    library(doParallel)
    registerDoParallel()

    x <- 1:10
    y <- rev(x)
    f <- function(x, y) if (x > y) stop("whooops") else x + y

    ## catch.errors = FALSE
    params <- list(
        batchjobs=BatchJobsParam(catch.errors=FALSE, progressbar=FALSE),
        dopar=DoparParam(catch.errors=FALSE),
        snow=SnowParam(catch.errors=FALSE),
        serial=SerialParam(catch.errors=FALSE))

    if (.Platform$OS.type != "windows")
        params <- c(params, multi=MulticoreParam(catch.errors=FALSE))

    for (param in params) {
        checkExceptionText(bpmapply(f, x, y, BPPARAM=param),
            "Error in FUN(...): whooops", negate=TRUE)
    }

    ## catch.errors = TRUE 
    params <- list(
        dopar=DoparParam(),
        snow=SnowParam(),
        serial=SerialParam())

    if (.Platform$OS.type != "windows")
        params <- c(params, multi=MulticoreParam())

    for (param in params) {
        res <- bplapply(list(1, "2", 3), sqrt, BPPARAM=param)
        checkTrue(length(res) == 3L)
        msg <- "non-numeric argument to mathematical function"
        checkIdentical(conditionMessage(res[[2]]), msg)
    }

    closeAllConnections()
    TRUE
}

test_bpresume <- function()
{
    ## BatchJobs only
    x <- 1:10
    y <- rev(x)
    f <- function(x, y) if (x > y) stop("whooops") else x + y
    f.fix <- function(x, y) 0

   params <- list(
        batchjobs=BatchJobsParam(catch.errors=TRUE, progressbar=FALSE))

    for (param in params) {
        ok <- try(bpmapply(f, x, y, BPPARAM=param), silent=TRUE)
        checkTrue(inherits(ok, "try-error"))

        ok <- try(bpresume(bpmapply(f, x, y, BPPARAM=param)), silent=TRUE)
        checkTrue(inherits(ok, "try-error"))

        res <- bpresume(bpmapply(f.fix, x, y, BPPARAM=param))
        checkIdentical(as.integer(res), c(rep(11L, 5), rep(0L, 5)))
    }

    closeAllConnections()
    TRUE
}
