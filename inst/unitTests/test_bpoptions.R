checkMsg <- function(x) {
    res <- capture.output(x, type = "message")
    checkTrue(length(res) > 0)
}

## Normal reduce process
test_bpoptions_constructor <- function() {
    opts <- bpoptions()
    checkIdentical(opts, list())

    opts <- bpoptions(tasks = 1)
    checkIdentical(opts, list(tasks = 1))

    checkMsg(opts <- bpoptions(randomArg = 1))
    checkIdentical(opts, list(randomArg = 1))

    checkMsg(opts <- bpoptions(tasks = 1, randomArg = 1))
    checkIdentical(opts, list(tasks = 1, randomArg = 1))
}

test_bpoptions_bplapply <- function() {
    p <- SerialParam()

    ## bpoptions only changes BPPARAM temporarily
    oldValue <- bptasks(p)
    opts <- bpoptions(tasks = 100)
    bplapply(1:2, function(x) x, BPPARAM = p, BPOPTIONS = opts)
    checkIdentical(bptasks(p), oldValue)

    ## check if bpoptions really works
    opts <- bpoptions(timeout = 1)
    checkException(
        bplapply(1:2, function(x) Sys.sleep(2), BPPARAM = p,
                 BPOPTIONS = opts)
    )

    ## Random argument has no effect on bplapply
    checkMsg(opts <- bpoptions(randomArg = 100))
    bplapply(1:2, function(x) x, BPPARAM = p, BPOPTIONS = opts)
}
