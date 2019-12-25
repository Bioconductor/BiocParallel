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

    ## no change when param started
    p <- PARAM(2)
    set.seed(100); p <- bpstart(p); obs <- runif(10)
    checkIdentical(exp, obs)

    ## no change when iterating over started param
    p <- bpstart(PARAM(2))
    set.seed(100); res <- bplapply(1:5, identity, BPPARAM = p); obs <- runif(10)
    checkIdentical(exp, obs)

    ## implicit TransientMulticoreParam (non-Windows)
    if (.Platform$OS.type != "windows") {
        p <- PARAM(2)
        set.seed(100); res <- bplapply(1:5, identity, BPPARAM = p)
        obs <- runif(10)
        checkIdentical(exp, obs)
    }
}
