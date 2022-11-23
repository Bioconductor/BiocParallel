message("Testing bpoptions")

.checkMessage <- function(x) {
    message <- character()
    result <- withCallingHandlers(x, message = function(condition) {
        message <<- c(message, conditionMessage(condition))
        invokeRestart("muffleMessage")
    })
    checkTrue(length(message) > 0)
}

## Normal reduce process
test_bpoptions_constructor <- function() {
    opts <- bpoptions()
    checkIdentical(opts, list())

    opts <- bpoptions(tasks = 1)
    checkIdentical(opts, list(tasks = 1))

    .checkMessage(opts <- bpoptions(randomArg = 1))
    checkIdentical(opts, list(randomArg = 1))

    .checkMessage(opts <- bpoptions(tasks = 1, randomArg = 1))
    checkIdentical(opts, list(tasks = 1, randomArg = 1))
}

test_bpoptions_bplapply <- function() {
    p <- SerialParam()

    ## bpoptions only changes BPPARAM temporarily
    oldValue <- bptasks(p)
    opts <- bpoptions(tasks = 100)
    result0 <- bplapply(1:2, function(x) x, BPPARAM = p, BPOPTIONS = opts)
    checkIdentical(bptasks(p), oldValue)

    ## check if bpoptions really works
    opts <- bpoptions(timeout = 1)
    checkException(
        bplapply(1:2, function(x) {
            t <- Sys.time()
            ## spin...
            while(difftime(Sys.time(), t) < 2) {}
        }, BPPARAM = p, BPOPTIONS = opts)
    )

    ## Random argument has no effect on bplapply
    .checkMessage(opts <- bpoptions(randomArg = 100))
    result1 <- bplapply(1:2, function(x) x, BPPARAM = p, BPOPTIONS = opts)
    checkIdentical(result0, result1)
}


test_bpoptions_manually_export <- function(){
    p <- SnowParam(2, exportglobals = FALSE)
    bpstart(p)
    on.exit(bpstop(p), add = TRUE)

    ## global variables that cannot be found by auto export
    bar <- function() x
    environment(bar) <- .GlobalEnv
    foo <- function(x) bar()
    environment(foo) <- .GlobalEnv
    assign("x", 10, envir = .GlobalEnv)
    assign("bar", bar, envir = .GlobalEnv)
    on.exit(rm(x, bar, envir = .GlobalEnv), add = TRUE)

    ## auto export would not work here
    bpexportvariables(p) <- FALSE
    checkException(bplapply(1:2, foo, BPPARAM = p), silent = TRUE)

    ## still not work as no auto export
    opts <- bpoptions(exports = "x")
    checkException(bplapply(1:2, foo, BPPARAM = p, BPOPTIONS = opts),
                   silent = TRUE)

    ## manually export all variables
    opts <- bpoptions(exports = c("x", "bar"))
    res <- bplapply(1:2, foo, BPPARAM = p, BPOPTIONS = opts)
    checkIdentical(res, rep(list(10), 2))

    ## enable auto export would not solve the problem
    bpexportvariables(p) <- TRUE
    checkException(bplapply(1:2, foo, BPPARAM = p), silent = TRUE)

    ## manually export the variables which is missing from auto export
    opts <- bpoptions(exports = "x")
    res <- bplapply(1:2, foo, BPPARAM = p, BPOPTIONS = opts)
    checkIdentical(res, rep(list(10), 2))

    ## manually export packages
    bar2 <- function(x) SerialParam()
    environment(bar2) <- .GlobalEnv
    foo2 <- function(x) bar2()
    environment(foo2) <- .GlobalEnv
    assign("x", 10, envir = .GlobalEnv)
    assign("bar2", bar2, envir = .GlobalEnv)
    on.exit(rm(bar2, envir = .GlobalEnv), add = TRUE)

    bpexportvariables(p) <- TRUE
    checkException(bplapply(1:2, foo2, BPPARAM = p), silent = TRUE)

    opts <- bpoptions(packages = c("BiocParallel"))
    res <- bplapply(1:2, foo2, BPPARAM = p, BPOPTIONS = opts)
    checkTrue(is(res[[1]], "SerialParam"))

    ## https://github.com/Bioconductor/BiocParallel/issues/234
    opts <- bpoptions(exports = "x")
    res <- bplapply(1:2, foo, BPPARAM = SerialParam(), BPOPTIONS = opts)
    checkIdentical(res, rep(list(10), 2))
    checkIdentical(.GlobalEnv[["x"]], 10)
}
