quiet <- suppressWarnings
.lazyCount <- function(count) {
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
        quiet(res <- bpiterate(ITER, FUN, BPPARAM=params1[[ptype]]))
        checkIdentical(expected, res)
    }

    for (ptype in names(params2)) {
        ITER <- .lazyCount(length(x))
        checkException(bpiterate(ITER, FUN, BPPARAM=params2[[ptype]]), 
                       silent=TRUE)
    }

    closeAllConnections()
    TRUE
}

test_bpiterate_REDUCE <- function() {

    workers <- 3
    param <- MulticoreParam(workers)

    ## no REDUCE
    FUN <- function(count, ...) rep(count, 10)
    ITER <- .lazyCount(workers)
    res <- bpiterate(ITER, FUN, BPPARAM=param)
    checkTrue(length(res) == 3L)
    expected <- list(rep(1L, 10), rep(2L, 10), rep(3L, 10))
    checkIdentical(expected, res)

    ## reduce.in.order=FALSE
    if (.Platform$OS.type != "windows") {
        FUN <- function(count, ...) rep(count, 10)
        ITER <- .lazyCount(workers)
        res <- bpiterate(ITER, FUN, BPPARAM=param, REDUCE=`+`)
        checkTrue(length(res) == 1L)
        expected <- list(rep(6L, 10))
        checkIdentical(expected, res)

        ## reduce.in.order=TRUE
        FUN <- function(count, ...) {
            if (count == 1)
                Sys.sleep(3)
            count
        }
        ITER <- .lazyCount(workers)
        res <- bpiterate(ITER, FUN, BPPARAM=param, REDUCE=paste0, 
                         reduce.in.order=FALSE)
        checkIdentical(unlist(res, use.names=FALSE), "231")

        ITER <- .lazyCount(workers)
        res <- bpiterate(ITER, FUN, BPPARAM=param, REDUCE=paste0, 
                         reduce.in.order=TRUE)
        checkIdentical(unlist(res, use.names=FALSE), "123")

        ITER <- .lazyCount(workers)
        res <- bpiterate(ITER, FUN, BPPARAM=param, REDUCE=paste0, 
                         init=0, reduce.in.order=TRUE)
        checkIdentical(unlist(res, use.names=FALSE), "0123")

        ITER <- .lazyCount(workers)
        res <- bpiterate(ITER, FUN, BPPARAM=param, REDUCE=paste0, 
                         init=0, reduce.in.order=FALSE)
        checkIdentical(unlist(res, use.names=FALSE), "0123")
    }

    closeAllConnections()
    TRUE
}

    workers <- 3
    FUN <- function(count, ...) rep(count, 10)
    param <- MulticoreParam(workers)



