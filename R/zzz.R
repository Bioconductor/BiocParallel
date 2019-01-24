.onLoad <-
    function(...)
{
    ## initial port assigned on load, rather than on first use
    ## provides consistency in random number seed advance, see
    ## https://stat.ethz.ch/pipermail/bioc-devel/2019-January/014526.html
    ##
    ## don't perturb the random number stream, see
    ## https://stat.ethz.ch/pipermail/bioc-devel/2019-January/014535.html
    oseed <- .GlobalEnv$.Random.seed
    on.exit({
        if (!is.null(oseed))
            .GlobalEnv$.Random.seed <- oseed
    })

    port <- new.env(parent = emptyenv(), size = 3L)
    port[["SnowParam"]] <- .snowPort()
    port[["MulticoreParam"]] <- .snowPort()
    .registry_port <<- port
}
