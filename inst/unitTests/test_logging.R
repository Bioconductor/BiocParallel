## This code tests 'log' and 'progressbar'. See test_errorhandling.R for
## tests with 'stop.on.error' and 'catch.errors'.

test_log <- function()
{
    ## SnowParam, MulticoreParam only
    params <- list(
        snow=SnowParam(log=FALSE),
        snowLog=SnowParam(log=TRUE))
    if (.Platform$OS.type != "windows") {
        params$multi=MulticoreParam(log=FALSE)
        params$multiLog=MulticoreParam(log=TRUE)
    }

    for (p in params) {
        res <- bplapply(list(1, "2", 3), sqrt, BPPARAM=p)
        checkTrue(length(res) == 3L)
        msg <- "non-numeric argument to mathematical function"
        checkIdentical(conditionMessage(res[[2]]), msg)
        checkTrue(length(attr(res[[2]], "traceback")) > 0L)
    }

    closeAllConnections()
    TRUE
}
