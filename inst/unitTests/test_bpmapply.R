message("Testing bpmapply")

quiet <- suppressWarnings

test_bpmapply_MoreArgs_names <- function()
{
    ## https://github.com/Bioconductor/BiocParallel/issues/51
    f <- function(x, y) x
    target <- bpmapply(f, 1:3, MoreArgs=list(x=1L))
    checkIdentical(rep(1L, 3), target)
}

test_bpmapply_Params <- function()
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
    y <- rev(x)
    f <- function(x, y) x + y
    expected <- x + y
    for (param in params) {
        current <- quiet(bpmapply(f, x, y, BPPARAM=param))
        checkIdentical(expected, current)
    }

    # test names and simplify
    x <- setNames(1:5, letters[1:5])
    for (param in params) {
        for (SIMPLIFY in c(FALSE, TRUE)) {
            for (USE.NAMES in c(FALSE, TRUE)) {
                expected <- mapply(identity, x,
                                   USE.NAMES=USE.NAMES,
                                   SIMPLIFY=SIMPLIFY)
                current <- quiet(bpmapply(identity, x,
                                          USE.NAMES=USE.NAMES,
                                          SIMPLIFY=SIMPLIFY,
                                          BPPARAM=param))
                checkIdentical(expected, current)
            }
        }
    }

    # test MoreArgs
    x <- setNames(1:5, letters[1:5])
    f <- function(x, m) { x + m }
    expected <- mapply(f, x, MoreArgs=list(m=1))
    for (param in params) {
        current <- quiet(bpmapply(f, x, MoreArgs=list(m=1), BPPARAM=param))
        checkIdentical(expected, current)
    }

    # test empty input
    for (param in params) {
        current <- quiet(bpmapply(identity, BPPARAM=param))
        checkIdentical(list(), current)
    }

    ## clean up
    foreach::registerDoSEQ()
    parallel::stopCluster(cl)
    closeAllConnections()
}

test_bpmapply_symbols <- function()
{
    cl <- parallel::makeCluster(2)
    doParallel::registerDoParallel(cl)
    params <- list(serial=SerialParam(),
                   snow=SnowParam(2),
                   dopar=DoparParam())
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2)

    x <- list(as.symbol(".XYZ"))
    expected <- mapply(as.character, x)
    for (param in names(params)) {
        current <- bpmapply(as.character, x, BPPARAM=params[[param]])
        checkIdentical(expected, current)
    }

    ## clean up
    foreach::registerDoSEQ()
    parallel::stopCluster(cl)
    closeAllConnections()
    TRUE
}

test_bpmapply_named_list <- function()
{
    X <- list()
    Y <- character()
    checkIdentical(X, bpmapply(identity))
    checkIdentical(X, bpmapply(identity, X))
    checkIdentical(mapply(identity, Y), bpmapply(identity, Y))

    checkIdentical(X, bpmapply(identity, USE.NAMES = FALSE))
    checkIdentical(X, bpmapply(identity, X, USE.NAMES = FALSE))
    checkIdentical(X, bpmapply(identity, Y, USE.NAMES = FALSE))

    names(X) <- names(Y) <- character()
    checkIdentical(X, bpmapply(identity, X))
    checkIdentical(X, bpmapply(identity, Y))

    checkIdentical(list(), bpmapply(identity, X, USE.NAMES = FALSE))
    checkIdentical(list(), bpmapply(identity, Y, USE.NAMES = FALSE))

    Y1 <- setNames(letters, letters)
    Y2 <- setNames(letters, LETTERS)
    checkIdentical(mapply(identity, Y1), bpmapply(identity, Y1))
    checkIdentical(mapply(identity, Y2), bpmapply(identity, Y2))

    X <- list(c(a = 1))
    checkIdentical(X, bpmapply(identity, X, SIMPLIFY = FALSE))

    X <- list(a = 1:2)
    checkIdentical(X, bpmapply(identity, X, SIMPLIFY = FALSE))

    X <- list(a = 1:2, b = 1:4)
    checkIdentical(X, bpmapply(identity, X, SIMPLIFY = FALSE))

    X <- list(A = c(a = 1:3))
    checkIdentical(X, bpmapply(identity, X, SIMPLIFY = FALSE))

    X <- list(A = c(a = 1, b=2), B = c(c = 1, d = 2))
    checkIdentical(X, bpmapply(identity, X, SIMPLIFY = FALSE))

    ## named arguments to bpmapply
    Y <- 1:3
    checkIdentical(Y, bpmapply(identity, x = Y))
}
