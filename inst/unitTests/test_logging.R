## This code tests 'log' and 'progressbar'. test_errorhandling.R
## tests 'stop.on.error' and 'catch.errors'.

test_log <- function()
{
    ## SnowParam, MulticoreParam only
    params <- list(
        snow=SnowParam(2, log=FALSE),
        snowLog=SnowParam(2, log=TRUE))
    if (.Platform$OS.type != "windows") {
        params$multi=MulticoreParam(3, log=FALSE)
        params$multiLog=MulticoreParam(3, log=TRUE)
    }

    for (param in params) {
        res <- bplapply(list(1, "2", 3), sqrt, BPPARAM=param)
        checkTrue(length(res) == 3L)
        msg <- "non-numeric argument to mathematical function"
        checkIdentical(conditionMessage(res[[2]]), msg)
        checkTrue(length(attr(res[[2]], "traceback")) > 0L)
        closeAllConnections()
    }

    ## clean up
    closeAllConnections()
    TRUE
}
