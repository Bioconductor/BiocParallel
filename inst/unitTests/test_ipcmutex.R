message("Testing ipcmutex")

test_ipclock <- function()
{
    id <- ipcid()
    on.exit(ipcremove(id))
    result <- bplapply(1:5, function(i, id) {
        BiocParallel::ipclock(id)
        Sys.sleep(.1)
        time <- Sys.time()
        BiocParallel::ipcunlock(id)
        time
    }, id, BPPARAM=SnowParam(2))
    d <- diff(range(unlist(result, use.names=FALSE)))
    checkTrue(d > 0.4)
}

test_ipccounter <- function()
{
    checkIdentical(ipcyield(ipcid()), 1L)
    id <- ipcid()
    on.exit(ipcremove(id))
    result <- bplapply(1:5, function(i, id) {
        BiocParallel::ipcyield(id)
    }, id, BPPARAM=SnowParam(2))
    checkIdentical(sort(unlist(result, use.names=FALSE)), 1:5)
}

test_ipc_errors <- function()
{
    ## Error : Expected 'character' actual 'double'
    checkException(ipclock(123))
    ## Error : 'id' must be character(1) and not NA
    checkException(ipclock(NA_character_))
    ## Error : 'id' must be character(1) and not NA
    checkException(ipclock(letters))

    ## expect no error
    id <- ipcid()
    ipcreset(id, 10)

    ## Error: Expected single integer value
    checkException(ipcreset(id, 1:3))
    ## Error: 'n' must not be NA
    checkException(ipcreset(id, NA_integer_))

    ipcremove(id)
}
