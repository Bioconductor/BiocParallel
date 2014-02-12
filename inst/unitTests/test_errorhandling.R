checkExceptionText <-
    function(expr, txt, negate=FALSE, msg="")
{
    x <- try(eval(expr), silent=TRUE)
    checkTrue(inherits(x, "try-error"), msg=msg)
    checkTrue(xor(negate, grepl(txt, as.character(x), fixed=TRUE)), msg=msg)
}

test_errorhandling <-
    function()
{
    # FIXME we need the windows workaround
    library(doParallel)
    registerDoParallel()

    x <- 1:10
    y <- rev(x)
    f <- function(x, y) if (x > y) stop("whooops") else x + y

    params <- list(serial=SerialParam(catch.errors=FALSE),
        snow0=SnowParam(2, "FORK", catch.errors=FALSE),
        snow1=SnowParam(2, "PSOCK", catch.errors=FALSE),
        batchjobs=BatchJobsParam(catch.errors=FALSE, progressbar=FALSE),
        dopar=DoparParam(catch.errors=FALSE),
        multi=MulticoreParam(catch.errors=FALSE))
    if (grepl("windows", .Platform$OS.type))
        params$snow0 <- NULL

    for (param in params) {
        checkExceptionText(bpmapply(f, x, y, BPPARAM=param),
            "Error in FUN(...): whooops", negate=TRUE)
    }

    params <- list(serial=SerialParam(catch.errors=TRUE),
        snow0=SnowParam(2, "FORK", catch.errors=TRUE),
        snow1=SnowParam(2, "PSOCK", catch.errors=TRUE),
        batchjobs=BatchJobsParam(catch.errors=TRUE, progressbar=FALSE),
        multi=MulticoreParam(catch.errors=TRUE),
        dopar=DoparParam(catch.errors=TRUE))
    if (grepl("windows", .Platform$OS.type))
        params$snow0 <- NULL
    for (param in params) {
        checkExceptionText(bpmapply(f, x, y, BPPARAM=param),
            "Error in FUN(...): whooops")
    }

    # check that resume works
    x <- 1:10
    y <- rev(x)
    f <- function(x, y) if (x > y) stop("whooops") else x + y
    f.fix <- function(x, y) 0


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
