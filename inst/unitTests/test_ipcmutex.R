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
    d <- diff(sort(unlist(result, use.names=FALSE)))
    checkEquals(d, rep(.1, 4), tolerance=.01)
}

test_ipccounter <- function()
{
    id <- ipcid()
    on.exit(ipcremove(id))
    result <- bplapply(1:5, function(i, id) {
        BiocParallel::ipcyield(id)
    }, id, BPPARAM=SnowParam(2))
    checkIdentical(sort(unlist(result, use.names=FALSE)), 1:5)
}
