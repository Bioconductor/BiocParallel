quiet <- suppressWarnings

test_bpmapply_MoreArgs_names <- function()
{
    ## https://github.com/Bioconductor/BiocParallel/issues/51
    f <- function(x, y) x
    target <- bpmapply(f, 1:3, MoreArgs=list(x=1L))
    checkIdentical(bpresult(rep(1L, 3)), target)
}

test_bpmapply_Params <- function() 
{
    if (.Platform$OS.type != "windows") {
        doParallel::registerDoParallel(2)
        params <- list(serial=SerialParam(),
                       snow=SnowParam(2),
                       dopar=DoparParam(),
                       batchjobs=BatchJobsParam(2, progressbar=FALSE),
                       mc <- MulticoreParam(2))

        x <- 1:10
        y <- rev(x)
        f <- function(x, y) x + y
        expected <- bpresult(x + y)
        for (param in params) {
            current <- quiet(bpmapply(f, x, y, BPPARAM=param))
            checkIdentical(expected, current)
            closeAllConnections()
        }

        # test names and simplify
        x <- setNames(1:5, letters[1:5])
        for (param in params) {
            for (SIMPLIFY in c(FALSE, TRUE)) {
                for (USE.NAMES in c(FALSE, TRUE)) {
                    expected <- mapply(identity, x, 
                                       USE.NAMES=USE.NAMES,
                                       SIMPLIFY=SIMPLIFY)
                    expected <- bpresult(expected)
                    current <- quiet(bpmapply(identity, x, 
                                              USE.NAMES=USE.NAMES,
                                              SIMPLIFY=SIMPLIFY, 
                                              BPPARAM=param))
                    checkIdentical(expected, current)
                    closeAllConnections()
                }
            }
        }

        # test MoreArgs
        x <- setNames(1:5, letters[1:5])
        f <- function(x, m) { x + m }
        expected <- bpresult(mapply(f, x, MoreArgs=list(m=1)))
        for (param in params) {
            current <- quiet(bpmapply(f, x, MoreArgs=list(m=1), BPPARAM=param))
            checkIdentical(expected, current)
            closeAllConnections()
        }

        # test empty input
        for (param in params) {
            current <- quiet(bpmapply(identity, BPPARAM=param))
            checkIdentical(bpresult(), current)
            closeAllConnections()
        }

        ## clean up
        env <- foreach:::.foreachGlobals
        rm(list=ls(name=env), pos=env)
        closeAllConnections()
        TRUE
    } else TRUE
}

test_bpmapply_symbols <- function()
{
    doParallel::registerDoParallel(2)
    params <- list(serial=SerialParam(),
                   snow=SnowParam(2),
                   dopar=DoparParam())
    if (.Platform$OS.type != "windows")
        params$mc <- MulticoreParam(2)

    x <- list(as.symbol(".XYZ"))
    expected <- bpresult(mapply(as.character, x))
    for (param in names(params)) {
        current <- bpmapply(as.character, x, BPPARAM=params[[param]])
        checkIdentical(expected, current)
    }

    ## clean up
    env <- foreach:::.foreachGlobals
    rm(list=ls(name=env), pos=env)
    closeAllConnections()
    TRUE
}

test_bpmapply_named_list <- function()
{
    X <- list()
    Y <- character()
    checkIdentical(bpresult(X), bpmapply(identity))
    checkIdentical(bpresult(X), bpmapply(identity, X))
    checkIdentical(bpresult(mapply(identity, Y)), bpmapply(identity, Y))

    checkIdentical(bpresult(X), bpmapply(identity, USE.NAMES = FALSE))
    checkIdentical(bpresult(X), bpmapply(identity, X, USE.NAMES = FALSE))
    checkIdentical(bpresult(X), bpmapply(identity, Y, USE.NAMES = FALSE))

    names(X) <- names(Y) <- character()
    checkIdentical(bpresult(X), bpmapply(identity, X))
    checkIdentical(bpresult(X), bpmapply(identity, Y))

    checkIdentical(bpresult(), bpmapply(identity, X, USE.NAMES = FALSE))
    checkIdentical(bpresult(), bpmapply(identity, Y, USE.NAMES = FALSE))

    Y1 <- setNames(letters, letters)
    Y2 <- setNames(letters, LETTERS)
    checkIdentical(bpresult(mapply(identity, Y1)), bpmapply(identity, Y1))
    checkIdentical(bpresult(mapply(identity, Y2)), bpmapply(identity, Y2))

    X <- bpresult(list(c(a = 1)))
    checkIdentical(X, bpmapply(identity, X, SIMPLIFY = FALSE))

    X <- bpresult(list(a = 1:2))
    checkIdentical(X, bpmapply(identity, X, SIMPLIFY = FALSE))

    X <- bpresult(list(a = 1:2, b = 1:4))
    checkIdentical(X, bpmapply(identity, X, SIMPLIFY = FALSE))

    X <- bpresult(list(A = c(a = 1:3)))
    checkIdentical(X, bpmapply(identity, X, SIMPLIFY = FALSE))

    X <- bpresult(list(A = c(a = 1, b=2), B = c(c = 1, d = 2)))
    checkIdentical(X, bpmapply(identity, X, SIMPLIFY = FALSE))

    ## named arguments to bpmapply
    Y <- bpresult(1:3)
    checkIdentical(Y, bpmapply(identity, x = Y))
}
