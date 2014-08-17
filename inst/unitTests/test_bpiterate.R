.lazyCount <- function(count)
{
    done <- FALSE
    count <- count
    i <- 0L

    function() {
        if (i >= count)
            return(NULL)
        else
            i <<- i + 1L
        i
    }
}

test_bpiterate_Params <- function()
{
    params1 <- list(serial=SerialParam(),
                   mc=MulticoreParam(2),
                   snow0=SnowParam(2, "FORK"),
                   snow1=SnowParam(2, "PSOCK"))
    params2 <- list(dopar=DoparParam(), 
                   batchjobs=BatchJobsParam())

    x <- 1:5
    expected <- lapply(x, sqrt)
    FUN <- function(count, ...) sqrt(count)
    for (ptype in names(params1)) {
        ITER <- .lazyCount(length(x))
        suppressWarnings(result <- bpiterate(ITER, FUN, 
                         BPPARAM=params1[[ptype]]))
        checkIdentical(expected, result)
    }

    for (ptype in names(params2)) {
        ITER <- .lazyCount(length(x))
        checkException(bpiterate(ITER, FUN, BPPARAM=params2[[ptype]]), 
                       silent=TRUE)
    }

    closeAllConnections()
    TRUE
}
