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
    ## chunks greater than no. workers
    x <- 1:5
    expected <- lapply(x, sqrt)
    FUN <- function(count, ...) sqrt(count)

    params <- list(serial=SerialParam(),
                   snow=SnowParam(2))
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2)
    for (p in params) {
        ITER <- .lazyCount(length(x))
        quiet(res <- bpiterate(ITER, FUN, BPPARAM=p))
        checkIdentical(expected, res)
    }

    ## chunks less than no. workers
    x <- 1:2
    expected <- lapply(x, sqrt)
    FUN <- function(count, ...) sqrt(count)
    params <- list(serial=SerialParam(),
                   snow=SnowParam(4))
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(4)

    for (p in params) {
        ITER <- .lazyCount(length(x))
        quiet(res <- bpiterate(ITER, FUN, BPPARAM=p))
        checkIdentical(expected, res)
    }

    params <- list(dopar=DoparParam(), batchjobs=BatchJobsParam())
    for (p in params) {
        ITER <- .lazyCount(length(x))
        checkException(bpiterate(ITER, FUN, BPPARAM=p), silent=TRUE)
    }

    closeAllConnections()
    TRUE
}

test_bpiterate_REDUCE <- function() {

    ncount <- 3L
    params <- list(snow=SnowParam(ncount))
    ## On Windows MulticoreParam dispatches to SerialParam where
    ## 'reduce.in.order' does not apply (always TRUE)
    if (.Platform$OS.type != "windows") 
        params <- c(params, multi=MulticoreParam(ncount))

    for (p in params) {
        ## no REDUCE
        FUN <- function(count, ...) rep(count, 10)
        ITER <- .lazyCount(ncount)
        res <- bpiterate(ITER, FUN, BPPARAM=p)
        checkTrue(length(res) == ncount)
        expected <- list(rep(1L, 10), rep(2L, 10), rep(3L, 10))
        checkIdentical(expected, res)

        ## REDUCE
        FUN <- function(count, ...) rep(count, 10)
        ITER <- .lazyCount(ncount)
        res <- bpiterate(ITER, FUN, BPPARAM=p, REDUCE=`+`)
        checkTrue(length(res) == 1L)
        expected <- list(rep(6L, 10))
        checkIdentical(expected, res)

        FUN <- function(count, ...) {
            Sys.sleep(3 - count)
            count
        }
        ## 'reduce.in.order' FALSE
        ITER <- .lazyCount(ncount)
        res <- bpiterate(ITER, FUN, BPPARAM=p, REDUCE=paste0, 
                         reduce.in.order=FALSE)
        checkIdentical(unlist(res, use.names=FALSE), "321")

        ITER <- .lazyCount(ncount)
        res <- quiet(bpiterate(ITER, FUN, BPPARAM=p, REDUCE=paste0, init=0, 
                               reduce.in.order=FALSE))
        checkIdentical(unlist(res, use.names=FALSE), "0321")

        ## 'reduce.in.order' TRUE 
        ITER <- .lazyCount(ncount)
        res <- bpiterate(ITER, FUN, BPPARAM=p, REDUCE=paste0, 
                         reduce.in.order=TRUE)
        checkIdentical(unlist(res, use.names=FALSE), "123")

        ITER <- .lazyCount(ncount)
        res <- bpiterate(ITER, FUN, BPPARAM=p, REDUCE=paste0, 
                         init=0, reduce.in.order=TRUE)
        checkIdentical(unlist(res, use.names=FALSE), "0123")
    }

    closeAllConnections()
    TRUE
}

