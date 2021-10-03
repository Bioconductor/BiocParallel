.bpinit <-
    function(manager, FUN, BPPARAM, ...)
{
    if (!bpisup(BPPARAM)) {
        ## start cluster.

        ## These conditions avoid cost of serializing data to workers
        ## when not necessary
        if (!inherits(BPPARAM, "SerialParam") &&
            (!bpschedule(BPPARAM) || bpnworkers(BPPARAM) == 1L)) {
            ## use SerialParam when only one worker
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
            BPPARAM <- as(BPPARAM, "TransientMulticoreParam")
            on.exit({
                .RNGstream(oldParam) <- .RNGstream(BPPARAM)
            }, TRUE, FALSE)
        }

        ## start / stop cluster
        BPPARAM <- bpstart(BPPARAM)
        on.exit(bpstop(BPPARAM), TRUE, FALSE)
    }

    ## FUN
    FUN <- .composeTry(
        FUN, bplog(BPPARAM), bpstopOnError(BPPARAM),
        timeout=bptimeout(BPPARAM), exportglobals=bpexportglobals(BPPARAM),
        force.GC = bpforceGC(BPPARAM)
    )

    ## iteration
    res <- bploop(
        manager, # dispatch
        FUN = FUN,
        BPPARAM = BPPARAM,
        ...
    )
    res
}

