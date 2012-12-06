library(doParallel)                     # FIXME: unload?

test_bpvec_Params <- function()
{
    params <- list(serial=SerialParam(),
                   mc=MulticoreParam(2),
                   snow0=SnowParam(2, "FORK"),
                   snow1=SnowParam(2, "PSOCK"),
                   dopar=DoparParam())

    dop <- registerDoParallel(cores=2)
    ## FIXME: restore previously registered back-end?

    x <- 1:10
    expected <- sqrt(x)
    for (ptype in names(params)) {
        obs <- bpvec(x, sqrt, param=params[[ptype]])
        checkIdentical(expected, obs)
    }
}
