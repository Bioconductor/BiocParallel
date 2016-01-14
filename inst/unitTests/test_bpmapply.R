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
        expected <- x + y
        for (param in params) {
            current <- quiet(bpmapply(f, x, y, BPPARAM=param))
            checkIdentical(expected, current)
            closeAllConnections()
        }

        # test names and simplify
        x <- setNames(1:5, letters[1:5])
        for (param in params) {
            for (catch.errors in c(FALSE, TRUE)) {
                bpcatchErrors(param) <- catch.errors
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
                      closeAllConnections()
                    }
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
            closeAllConnections()
        }

        # test empty input
        for (param in params) {
            current <- quiet(bpmapply(identity, BPPARAM=param))
            checkIdentical(list(), current)
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
    expected <- mapply(as.character, x)
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
