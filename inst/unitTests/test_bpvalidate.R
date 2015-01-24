test_bpvalidate_library <- function()
{
    fun <- function(fl, ...) {
        library(Rsamtools) 
        countBam(fl)
    }
    res <- bpvalidate(fun) ## loads Rsamtools
    checkIdentical(res$RequiredPackages, "Rsamtools")
    checkIdentical(res$UnknownSymbols, character())
}

test_bpvalidate_args <- function()
{
    library(Rsamtools)
    param <- ScanBamParam(flag=scanBamFlag(isMinusStrand=FALSE))
    fun <- function(fl, ...) {
        library(Rsamtools) 
        countBam(fl, param=param)
    }
    res <- suppressWarnings(bpvalidate(fun))
    checkIdentical(res$RequiredPackages, "Rsamtools")
    checkIdentical(res$UnknownSymbols, "param")

    fun <- function(fl, ..., param) {
        library(Rsamtools) 
        countBam(fl, param=param)
    }
    res <- bpvalidate(fun)
    checkIdentical(res$RequiredPackages, "Rsamtools")
    checkIdentical(res$UnknownSymbols, character())
}
