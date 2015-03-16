library(Rsamtools)
test_bpvalidate_library <- function()
{
    fun <- function(fl, ...) {
        countBam(fl)
    }
    res <- suppressMessages(bpvalidate(fun))
    checkIdentical(names(res$inPath), "countBam")
    checkIdentical(res$unknown, character())
}

test_bpvalidate_args <- function()
{
    param <- ScanBamParam(flag=scanBamFlag(isMinusStrand=FALSE))
    fun <- function(fl, ...) {
        library(Rsamtools) 
        countBam(fl, param=param)
    }
    res <- bpvalidate(fun)
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
