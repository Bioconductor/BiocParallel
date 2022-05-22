message("Testing bpiterate")

quiet <- suppressWarnings

.lazyCount <- function(count) {
    i <- 0L
    function() {
        if (i >= count)
            return(NULL)
        i <<- i + 1L
        i
    }
}

test_bpiterate_Params <- function()
{
    ## chunks greater than number of workers
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

    ## chunks less than number of workers
    x <- 1:2
    expected <- lapply(x, sqrt)
    FUN <- function(count, ...) sqrt(count)
    params <- list(serial=SerialParam(),
                   snow=SnowParam(3))
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(3)

    for (p in params) {
        ITER <- .lazyCount(length(x))
        quiet(res <- bpiterate(ITER, FUN, BPPARAM=p))
        checkIdentical(expected, res)
    }

    cl <- parallel::makeCluster(2)
    doParallel::registerDoParallel(cl)
    params <- list(dopar=DoparParam(),
                   batchjobs=BatchJobsParam(2, progressbar=FALSE))
    for (p in params) {
        ITER <- .lazyCount(length(x))
        checkException(bpiterate(ITER, FUN, BPPARAM=p), silent=TRUE)
    }

    ## clean up
    foreach::registerDoSEQ()
    parallel::stopCluster(cl)
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
        checkIdentical(rep(6L, 10), res)

        FUN <- function(count, ...) {
            Sys.sleep(3 - count)
            count
        }
        ## 'reduce.in.order' FALSE
        ITER <- .lazyCount(ncount)
        res <- bpiterate(ITER, FUN, BPPARAM=p, REDUCE=paste0,
                         reduce.in.order=FALSE)
        checkIdentical("321", res)

        ITER <- .lazyCount(ncount)
        res <- quiet(bpiterate(ITER, FUN, BPPARAM=p, REDUCE=paste0, init=0,
                               reduce.in.order=FALSE))
        checkIdentical("0321", res)

        ## 'reduce.in.order' TRUE
        ITER <- .lazyCount(ncount)
        res <- bpiterate(ITER, FUN, BPPARAM=p, REDUCE=paste0,
                         reduce.in.order=TRUE)
        checkIdentical("123", res)

        ITER <- .lazyCount(ncount)
        res <- bpiterate(ITER, FUN, BPPARAM=p, REDUCE=paste0,
                         init=0, reduce.in.order=TRUE)
        checkIdentical("0123", res)
    }

    ## clean up
    closeAllConnections()
    TRUE
}

test_bpiterate_REDUCE_SerialParam <- function() {
    p <- SerialParam()
    FUN <- identity

    ## REDUCE missing, concatenate
    ITER <- .lazyCount(0)
    res <- suppressWarnings({
        ## warning: first invocation of 'ITER()' returned NULL
        bpiterate(ITER, FUN, BPPARAM=p)
    })
    checkIdentical(list(), res)

    ITER <- .lazyCount(1)
    res <- bpiterate(ITER, FUN, BPPARAM=p)
    checkIdentical(list(1L), res)

    ITER <- .lazyCount(5)
    res <- bpiterate(ITER, FUN, BPPARAM=p)
    checkIdentical(as.list(1:5), res)

    ## REDUCE == `+`
    ITER <- .lazyCount(0)
    res <- suppressWarnings({
        ## warning: first invocation of 'ITER()' returned NULL
        res <- bpiterate(ITER, FUN, BPPARAM=p, REDUCE=`+`)
    })
    checkIdentical(NULL, res)

    ITER <- .lazyCount(1)
    res <- bpiterate(ITER, FUN, BPPARAM=p, REDUCE=`+`)
    checkIdentical(1L, res)

    ITER <- .lazyCount(5)
    res <- bpiterate(ITER, FUN, BPPARAM=p, REDUCE=`+`)
    checkIdentical(15L, res)
}
