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
    params <- list(
        serial=SerialParam(),
        snow=SnowParam(2),
        dopar=DoparParam()
    )
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

test_transposeArgsWithIterations <- function() {
    .transposeArgsWithIterations <- BiocParallel:::.transposeArgsWithIterations

    ## list() when `mapply()` invoked with no arguments, `mapply(identity)`
    checkIdentical(
        list(),
        .transposeArgsWithIterations(list(), USE.NAMES = TRUE)
    )
    checkIdentical(
        list(),
        .transposeArgsWithIterations(list(), USE.NAMES = FALSE)
    )

    ## list(X) when `mapply()` invoked with one argument, `mapply(identity, X)`
    X <- list()
    XX <- list(X)
    checkIdentical(list(), .transposeArgsWithIterations(XX, USE.NAMES = TRUE))
    checkIdentical(list(), .transposeArgsWithIterations(XX, USE.NAMES = FALSE))

    ## `mapply(identity, character())` returns a _named_ list()
    X <- character()
    XX <- list(X)
    checkIdentical(
        setNames(list(), character()),
        .transposeArgsWithIterations(XX, TRUE)
    )
    checkIdentical(list(), .transposeArgsWithIterations(XX, FALSE))

    ## named arguments to mapply() are _not_ names of return value...
    X <- list()
    XX <- list(x = X)
    checkIdentical(list(), .transposeArgsWithIterations(XX, TRUE))
    checkIdentical(list(), .transposeArgsWithIterations(XX, FALSE))

    ## ...except if the argument is a character()
    X <- character()
    XX <- list(x = X)
    checkIdentical(
        setNames(list(), character()),
        .transposeArgsWithIterations(XX, TRUE)
    )
    checkIdentical(list(), .transposeArgsWithIterations(XX, FALSE))

    ## with multiple arguments, names are from the first argument
    XX <- list(c(a = 1, b = 2, c = 3), c(d = 4, e = 5, f = 6))
    checkIdentical(
        setNames(list(list(1, 4), list(2, 5), list(3, 6)), letters[1:3]),
        .transposeArgsWithIterations(XX, TRUE)
    )
    checkIdentical(
        list(list(1, 4), list(2, 5), list(3, 6)),
        .transposeArgsWithIterations(XX, FALSE)
    )
  
    ## ...independent of names on the arguments
    XX <- list(A = c(a = 1, b = 2, c = 3), B = c(d = 4, e = 5, f = 6))
    checkIdentical(
        list(a = list(A=1, B=4), b = list(A=2, B=5), c = list(A=3, B=6)),
        .transposeArgsWithIterations(XX, TRUE)
    )
    checkIdentical(
        list(list(A=1, B=4), list(A=2, B=5), list(A=3, B=6)),
        .transposeArgsWithIterations(XX, FALSE)
    )

    ## when the first argument is an unnamed character vector, names
    ## are values
    XX <- list(A = c("a", "b", "c"), B = 1:3)
    checkIdentical(
        list(
            a = list(A="a", B=1L), b = list(A="b", B=2L), c = list(A="c", B=3L)
        ),
        .transposeArgsWithIterations(XX, TRUE)
    )
    checkIdentical(
        list(list(A="a", B=1L), list(A="b", B=2L), list(A="c", B=3L)),
        .transposeArgsWithIterations(XX, FALSE)
    )

    ## ...except if there are names on the first vector...
    XX <- list(A = setNames(letters[1:3], LETTERS[1:3]), B = 1:3)
    checkIdentical(
        list(
            A = list(A="a", B=1L), B = list(A="b", B=2L), C = list(A="c", B=3L)
        ),
        .transposeArgsWithIterations(XX, TRUE)
    )
    checkIdentical(
        list(list(A="a", B=1L), list(A="b", B=2L), list(A="c", B=3L)),
        .transposeArgsWithIterations(XX, FALSE)
    )
}
