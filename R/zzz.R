.onLoad <-
    function(...)
{
    ## initial port assigned on load, rather than on first use
    ## provides consistency in random number seed advance, see
    ## https://stat.ethz.ch/pipermail/bioc-devel/2019-January/014526.html
    port <- new.env(parent = emptyenv(), size = 3L)
    port[["SnowParam"]] <- .snowPort()
    port[["MulticoreParam"]] <- .snowPort()
    .registry_port <<- port
}
