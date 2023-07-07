.bpinit <-
    function(manager, BPPARAM, BPOPTIONS, ...)
{
    ## temporarily change the paramters in BPPARAM
    oldOptions <- .bpparamOptions(BPPARAM, names(BPOPTIONS))
    on.exit(.bpparamOptions(BPPARAM) <- oldOptions, TRUE, FALSE)
    .bpparamOptions(BPPARAM) <- BPOPTIONS

    ## fallback conditions(all must be satisfied):
    ## 1. BPPARAM has not been started
    ## 2. fallback is allowed (bpfallback(x) == TRUE)
    ## 3. One of the following conditions is met:
    ##   3.1 the worker number is less than or equal to 1
    ##   3.2 Parallel evaluation is disallowed (bpschedule(BPPARAM) == FALSE)
    ##   3.3 BPPARAM is of MulticoreParam class
    if (!bpisup(BPPARAM) && bpfallback(BPPARAM)) {
        ## use cases:
        ## bpnworkers: no worker available, or no benefit in parallel evaluation
        ## bpschedule: in nested parallel call where the same
        ##             BPPARAM cannot be reused
        if (bpnworkers(BPPARAM) <= 1L || !bpschedule(BPPARAM)) {
            oldParam <- BPPARAM
            BPPARAM <- as(BPPARAM, "SerialParam")
            on.exit({
                .RNGstream(oldParam) <- .RNGstream(BPPARAM)
            }, TRUE, FALSE) # add = TRUE, last = FALSE --> last in,
                            # first out order
        } else if (is(BPPARAM, "MulticoreParam")) {
            ## use TransientMulticoreParam when MulticoreParam has not
            ## started
            oldParam <- BPPARAM
            BPPARAM <- TransientMulticoreParam(BPPARAM)
            on.exit({
                .RNGstream(oldParam) <- .RNGstream(BPPARAM)
            }, TRUE, FALSE)
        }
    }

    ## start the BPPARAM if haven't
    if (!bpisup(BPPARAM)) {
        ## start / stop cluster
        BPPARAM <- bpstart(BPPARAM)
        on.exit(bpstop(BPPARAM), TRUE, FALSE)
    }

    ## iteration
    res <- bploop(
        manager, # dispatch
        BPPARAM = BPPARAM,
        BPOPTIONS = BPOPTIONS,
        ...
    )

    if (!.bpallok(res, attrOnly = TRUE))
        stop(.error_bplist(res))
    res
}

