.onLoad <-
    function(libname, pkgname)
{
    ## only SnowParam on widows, which is then the default (first)
    register(getOption("SnowParam", SnowParam(workers=detectCores())))
    if (.Platform$OS.type != "windows")
        register(getOption("MulticoreParam", MulticoreParam()))
}
