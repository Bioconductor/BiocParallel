message("Testing logging")

## This code tests 'log' and 'progressbar'. test_errorhandling.R
## tests 'stop.on.error'

test_log <- function()
{
    ## SnowParam, MulticoreParam only
    params <- list(
        snow=SnowParam(2, log=FALSE, stop.on.error=FALSE),
        snowLog=SnowParam(2, log=TRUE, stop.on.error=FALSE))
    if (.Platform$OS.type != "windows") {
        params$multi=MulticoreParam(3, log=FALSE, stop.on.error=FALSE)
        params$multiLog=MulticoreParam(3, log=TRUE, stop.on.error=FALSE)
    }

    for (param in params) {
        res <- suppressMessages(tryCatch({
            bplapply(list(1, "2", 3), sqrt, BPPARAM=param)
        }, error=identity))
        checkTrue(is(res, "bplist_error"))
        result <- bpresult(res)
        checkTrue(length(result) == 3L)
        msg <- "non-numeric argument to mathematical function"
        checkIdentical(conditionMessage(result[[2]]), msg)
        checkTrue(length(attr(result[[2]], "traceback")) > 0L)
    }

    ## clean up
    closeAllConnections()
    TRUE
}
