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
    message("test_bpiterate_Params")
    ## chunks greater than number of workers
    message("  chunks greater than number of workers")
    x <- 1:5
    expected <- lapply(x, sqrt)
    FUN <- function(count, ...) sqrt(count)

    params <- list(serial=SerialParam(),
                   snow=SnowParam(2))
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2)
    for (p in params) {
        message("    ith param")
        ITER <- .lazyCount(length(x))
        quiet(res <- bpiterate(ITER, FUN, BPPARAM=p))
        checkIdentical(expected, res)
    }

    ## chunks less than number of workers
    message("  chunks less than number of workers [1]")
    x <- 1:2
    expected <- lapply(x, sqrt)
    FUN <- function(count, ...) sqrt(count)
    params <- list(serial=SerialParam(),
                   snow=SnowParam(3))
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(3)

    for (p in params) {
        message("    ith param")
        ITER <- .lazyCount(length(x))
        quiet(res <- bpiterate(ITER, FUN, BPPARAM=p))
        checkIdentical(expected, res)
    }

    message("  chunks less than number of workers [2]")
    doParallel::registerDoParallel(2)
    params <- list(dopar=DoparParam(),
                   batchjobs=BatchJobsParam(2, progressbar=FALSE))
    for (p in params) {
        message("    ith param")
        ITER <- .lazyCount(length(x))
        checkException(bpiterate(ITER, FUN, BPPARAM=p), silent=TRUE)
    }

    ## clean up
    message("  cleanup")
    env <- foreach:::.foreachGlobals
    rm(list=ls(name=env), pos=env)
    closeAllConnections()
    TRUE
    message("test_bpiterate_Params DONE")
}

test_bpiterate_REDUCE <- function() {
    message("test_bpiterate_REDUCE")
    message("  setup")
    ncount <- 3L
    params <- list(snow=SnowParam(ncount))
    ## On Windows MulticoreParam dispatches to SerialParam where
    ## 'reduce.in.order' does not apply (always TRUE)
    if (.Platform$OS.type != "windows") 
        params <- c(params, multi=MulticoreParam(ncount))

    for (p in params) {
        message("    ith param")
        ## no REDUCE
        message("      no REDUCE")
        FUN <- function(count, ...) rep(count, 10)
        ITER <- .lazyCount(ncount)
        res <- bpiterate(ITER, FUN, BPPARAM=p)
        checkTrue(length(res) == ncount)
        expected <- list(rep(1L, 10), rep(2L, 10), rep(3L, 10))
        checkIdentical(expected, res)

        ## REDUCE
        message("      REDUCE")
        FUN <- function(count, ...) rep(count, 10)
        ITER <- .lazyCount(ncount)
        res <- bpiterate(ITER, FUN, BPPARAM=p, REDUCE=`+`)
        checkIdentical(rep(6L, 10), res)

        FUN <- function(count, ...) {
            Sys.sleep(3 - count)
            count
        }
        ## 'reduce.in.order' FALSE
        message("      'reduce.in.order' FALSE [1]")
        ITER <- .lazyCount(ncount)
        res <- bpiterate(ITER, FUN, BPPARAM=p, REDUCE=paste0, 
                         reduce.in.order=FALSE)
        checkIdentical("321", res)

        message("      'reduce.in.order' FALSE [2]")
        ITER <- .lazyCount(ncount)
        res <- quiet(bpiterate(ITER, FUN, BPPARAM=p, REDUCE=paste0, init=0, 
                               reduce.in.order=FALSE))
        checkIdentical("0321", res)

        ## 'reduce.in.order' TRUE 
        message("      'reduce.in.order' TRUE [1]")
        ITER <- .lazyCount(ncount)
        res <- bpiterate(ITER, FUN, BPPARAM=p, REDUCE=paste0, 
                         reduce.in.order=TRUE)
        checkIdentical("123", res)

        message("      'reduce.in.order' TRUE [2]")
        ITER <- .lazyCount(ncount)
        res <- bpiterate(ITER, FUN, BPPARAM=p, REDUCE=paste0, 
                         init=0, reduce.in.order=TRUE)
        checkIdentical("0123", res)
    }

    ## clean up
    message("  cleanup")
    closeAllConnections()
    TRUE

    message("test_bpiterate_REDUCE DONE")
}
