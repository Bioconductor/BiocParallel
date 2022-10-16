message("Testing internal_rng_stream")

test_internal_rng_stream <-
    function()
{
    if (.Platform$OS.type == "windows") {
        PARAM <- SnowParam
    } else {
        PARAM <- MulticoreParam
    }
    set.seed(100); exp <- runif(10)

    ## no change when param created (port assignment)
    set.seed(100); p <- PARAM(2); obs <- runif(10)
    checkIdentical(exp, obs)
}
