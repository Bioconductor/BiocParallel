.n_connections <- function()
    NROW(showConnections())

.counter <- function(count) {
    i <- 0L
    function() {
        if (i >= count)
            return(NULL)
        i <<- i + 1L
        i
    }
}

test_LocalParam_construction <- function() {
    checkTrue(validObject(LocalParam()))
    checkIdentical(2L, bpnworkers(LocalParam(2)))
    checkIdentical("test", bpjobname(LocalParam(jobname="test")))
}

test_LocalParam_startstop <- function() {
    n <- .n_connections()
    p <- LocalParam(2L)
    checkIdentical(FALSE, bpisup(p))
    bpstart(p)
    checkIdentical(TRUE, bpisup(p))
    checkIdentical(.n_connections(), n + 1L)
    bpstop(p)
    checkIdentical(FALSE, bpisup(p))
    checkIdentical(.n_connections(), n)
}

test_LocalParam_bplapply <- function() {
    n <- .n_connections()
    p <- LocalParam(2L)
    res <- bplapply(1:4, function(i) Sys.getpid(), BPPARAM = p)
    checkIdentical(2L, length(unique(res)))
    checkIdentical(.n_connections(), n)
    if (identical(.Platform$OS.type, "unix")) {
        p <- LocalParam(2L, cluster = "snow")
        res <- bplapply(1:4, function(i) Sys.getpid(), BPPARAM = p)
        checkIdentical(2L, length(unique(res)))
        checkIdentical(.n_connections(), n)
    }
}

test_LocalParam_bpiterate <- function() {
    n <- .n_connections()
    p <- LocalParam(2L)
    cnt <- .counter(5L)
    res <- bpiterate(cnt, function(i) Sys.getpid(), BPPARAM = p)
    checkIdentical(5L, length(res))
    checkIdentical(2L, length(unique(res)))
    checkIdentical(.n_connections(), n)
}
