test_bpvalidate_library <- function()
{
    .test_env <- new.env(parent=emptyenv())
    .test_env$countBam <- function(...) {}
    attach(.test_env)
    on.exit(detach(.test_env))
    fun <- function(fl, ...) {
        countBam(fl)
    }
    res <- suppressWarnings(bpvalidate(fun))
    checkIdentical(names(res$inPath), "countBam")
    checkIdentical(res$unknown, character())
}

test_bpvalidate_args <- function()
{
    param <- Rsamtools::ScanBamParam(
        flag=Rsamtools::scanBamFlag(isMinusStrand=FALSE))
    fun <- function(fl, ...) {
        library(Rsamtools) 
        countBam(fl, param=param)
    }
    res <- suppressWarnings(bpvalidate(fun))
    checkIdentical(unname(res$inPath), list())
    checkIdentical(res$unknown, "param") ## no .GlobalEnv; param -> unknown

    fun <- function(fl, ..., param) {
        library(Rsamtools) 
        countBam(fl, param=param)
    }
    res <- bpvalidate(fun)
    checkIdentical(unname(res$inPath), list())
    checkIdentical(res$unknown, character())
}
