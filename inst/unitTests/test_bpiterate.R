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
    x <- 1:5
    expected <- lapply(x, sqrt)
    FUN <- function(count, ...) sqrt(count)

    params <- list(serial=SerialParam(),
                   multi=MulticoreParam(2),
                   snow1=SnowParam(2, "SOCK"),
                   snow2=SnowParam(2, "MPI"))
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
    params <- list(snow=SnowParam(ncount), multi=MulticoreParam(ncount))

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
        ITER <- .lazyCount(ncount)
        res <- bpiterate(ITER, FUN, BPPARAM=p, REDUCE=paste0, 
                         reduce.in.order=FALSE)
        checkIdentical(unlist(res, use.names=FALSE), "321")

        ITER <- .lazyCount(ncount)
        res <- quiet(bpiterate(ITER, FUN, BPPARAM=p, REDUCE=paste0, init=0, 
                               reduce.in.order=FALSE))
        checkIdentical(unlist(res, use.names=FALSE), "0321")

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
