message("Testing bplapply")

quiet <- suppressWarnings

test_bplapply_Params <- function()
{
    cl <- parallel::makeCluster(2)
    doParallel::registerDoParallel(cl)
    params <- list(serial=SerialParam(),
                   snow=SnowParam(2),
                   dopar=DoparParam(),
                   batchjobs=BatchJobsParam(2, progressbar=FALSE))
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2)

    x <- 1:10
    expected <- lapply(x, sqrt)
    for (param in names(params)) {
        current <- quiet(bplapply(x, sqrt, BPPARAM=params[[param]]))
        checkIdentical(expected, current)
    }

    # test empty input
    for (param in names(params)) {
        current <- quiet(bplapply(list(), identity, BPPARAM=params[[param]]))
        checkIdentical(list(), current)
    }

    # unnamed args for BatchJobs -> dispatches to batchMap
    f <- function(i, x, y, ...) { list(y, i, x) }
    current <- bplapply(2:1, f, c("A", "B"), x=10,
                        BPPARAM=BatchJobsParam(2, progressbar=FALSE))
    checkTrue(all.equal(current[[1]], list(c("A", "B"), 2, 10)))
    checkTrue(all.equal(current[[2]], list(c("A", "B"), 1, 10)))

    ## clean up
    foreach::registerDoSEQ()
    parallel::stopCluster(cl)
    closeAllConnections()
    TRUE
}

test_bplapply_symbols <- function()
{
    cl <- parallel::makeCluster(2)
    doParallel::registerDoParallel(cl)
    params <- list(serial=SerialParam(),
                   snow=SnowParam(2),
                   dopar=DoparParam()
                   ## FIXME, batchjobs=BatchJobsParam(2, progressbar=FALSE))
                   )
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2)

    x <- list(as.symbol(".XYZ"))
    expected <- lapply(x, as.character)
    for (param in names(params)) {
        current <- quiet(bplapply(x, as.character, BPPARAM=params[[param]]))
        checkIdentical(expected, current)
    }

    ## clean up
    foreach::registerDoSEQ()
    parallel::stopCluster(cl)
    closeAllConnections()
    TRUE
}

test_bplapply_named_list <- function()
{
    X <- list()
    Y <- character()
    checkIdentical(X, bplapply(X, identity))
    checkIdentical(X, bplapply(Y, identity))

    names(X) <- names(Y) <- character()
    checkIdentical(X, bplapply(X, identity))
    checkIdentical(X, bplapply(Y, identity))

    X <- list(a = 1:2)
    checkIdentical(X, bplapply(X, identity))

    X <- list(c(a = 1))
    checkIdentical(X, bplapply(X, identity))

    X <- list(A = c(a = 1:2, b = 1:3), B = c(b = 1:2))
    checkIdentical(X, bplapply(X, identity))

    X <- list(a = 1:2, b = 3:4)
    checkIdentical(X, bplapply(X, identity))

    X <- list(c(a = 1))
    checkIdentical(X, bplapply(X, identity))

    X <- list(A = c(a = 1, b=2), B = c(c = 1, d = 2))
    checkIdentical(X, bplapply(X, identity))
}

test_bplapply_named_list_with_REDO <- function(){
    X = setNames(1:3, letters[1:3])
    param = SerialParam(stop.on.error = FALSE)
    FUN1 = function(i) if (i == 2) stop() else i
    result <- bptry(bplapply(X, FUN1, BPPARAM = param))
    checkIdentical(names(result), names(X))

    FUN2 = function(i) 0
    redo <- bplapply(X, FUN2, BPREDO = result, BPPARAM = param)
    checkIdentical(names(redo), names(X))
}

test_bplapply_custom_subsetting <- function(){
    ## We have a class A in the previous unit test
    .B <- setClass("B", slots = c(b = "integer"))
    setMethod("[", "B", function(x, i, j, ...) initialize(x, b = x@b[i]))
    setMethod("length", "B", function(x) length(x@b))
    as.list.B <<- function(x, ...) lapply(seq_along(x), function(i) x[i])

    x <- .B(b = 1:3)
    expected <- lapply(x, function(elt) elt@b)
    current <- quiet(bplapply(x, function(elt) elt@b, BPPARAM=SerialParam()))
    checkIdentical(expected, current)

    ## Remote worker does not have the definition of the class B
    res <- tryCatch(
        bplapply(x, function(elt) elt@b, BPPARAM=SnowParam(workers = 2)),
        error = identity
    )
    checkTrue(is(res, "bplist_error"))

    rm(as.list.B, inherits = TRUE)
}

test_bplapply_auto_export <- function(){
    p <- SnowParam(2, exportglobals = FALSE)

    ## user defined symbols
    assign("y", 10, envir = .GlobalEnv)
    on.exit(rm(y, envir = .GlobalEnv))
    fun <- function(x) y
    environment(fun) <- .GlobalEnv

    bpexportvariables(p) <- TRUE
    res <- bplapply(1:2, fun, BPPARAM = p)
    checkIdentical(res, rep(list(10), 2))

    bpexportvariables(p) <- FALSE
    checkException(bplapply(1:2, fun, BPPARAM = p), silent = TRUE)

    ## symbols defined in a package
    fun2 <- function(x) SerialParam()
    environment(fun2) <- .GlobalEnv
    bpexportvariables(p) <- TRUE
    res <- bplapply(1:2, fun2, BPPARAM = p)
    checkTrue(is(res[[1]], "SerialParam"))

    bpexportvariables(p) <- FALSE
    checkException(bplapply(1:2, fun2, BPPARAM = p), silent = TRUE)
}
