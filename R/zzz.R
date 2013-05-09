.splitIndices <- function (nx, ncl)
{
    ## derived from parallel
    i <- seq_len(nx)
    if (ncl <= 1L || nx <= 1L)          # allow nx, nc == 0
        i
    else {
        fuzz <- min((nx - 1L)/1000, 0.4 * nx/ncl)
        breaks <- seq(1 - fuzz, nx + fuzz, length = ncl + 1L)
        structure(split(i, cut(i, breaks)), names = NULL)
    }
}

.onLoad <-
    function(libname, pkgname)
{
    ## only SnowParam on widows, which is then the default (first)
    register(SerialParam())
    ## register(getOption("SnowParam", SnowParam(workers=detectCores())))
    ## if (.Platform$OS.type != "windows")
    ##     register(getOption("MulticoreParam", MulticoreParam()))
}
