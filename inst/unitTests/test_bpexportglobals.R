message("Testing bpexportglobals")

test_bpexportglobals_params <- function()
{
    ## Multicore
    if (.Platform$OS.type == "unix") {
        param <- MulticoreParam()
        checkIdentical(bpexportglobals(param), TRUE)
        bpexportglobals(param) <- FALSE
        checkIdentical(bpexportglobals(param), FALSE)
        param <- MulticoreParam(exportglobals=FALSE)
        checkIdentical(bpexportglobals(param), FALSE)
    }

    ## Snow
    param <- SnowParam()
    checkIdentical(bpexportglobals(param), TRUE)
    bpexportglobals(param) <- FALSE
    checkIdentical(bpexportglobals(param), FALSE)
    param <- SnowParam(exportglobals=FALSE)
    checkIdentical(bpexportglobals(param), FALSE)

    ## Batchtools
    param <- BatchtoolsParam()
    checkIdentical(bpexportglobals(param), TRUE)
    bpexportglobals(param) <- FALSE
    checkIdentical(bpexportglobals(param), FALSE)
    param <- BatchtoolsParam(exportglobals=FALSE)
    checkIdentical(bpexportglobals(param), FALSE)
}

test_bpexportglobals_bplapply <- function()
{
    oopts <- options(BAR="baz")
    on.exit(options(oopts))

    param <- SnowParam(2L, exportglobals=FALSE)
    current <- bplapply(1:2, function(i) getOption("BAR"), BPPARAM=param)
    checkIdentical(NULL, unlist(current))

    param <- SnowParam(2L, exportglobals=TRUE)
    current <- bplapply(1:2, function(i) getOption("BAR"), BPPARAM=param)
    checkIdentical("baz", unique(unlist(current)))
}

test_bpexportglobals_lazyEvaluation <- function(){
    foo <- function(k){
        param <- SnowParam(2L, exportglobals=TRUE)
        bplapply(1:2, function(x){
            k
        }, BPPARAM = param)
    }
    k <- 1
    checkIdentical(foo(k), list(1, 1))
}
