.bpinit <-
    function(manager, FUN, BPPARAM, ...)
{
    fallback <- FALSE
    if (!inherits(BPPARAM, "SerialParam")) {
        if (!bpschedule(BPPARAM) || bpnworkers(BPPARAM) == 1L) {
            fallback <- TRUE
            oldParam <- BPPARAM
            BPPARAM <- as(BPPARAM, "SerialParam")
        }
    }

    ## start / stop cluster
    if (!bpisup(BPPARAM)) {
        BPPARAM <- bpstart(BPPARAM)
        ## the fallback SerialParam must inherit the seed stream from
        ## BPPARAM
        if (fallback && bpisup(oldParam))
            .RNGstream(BPPARAM) <- .RNGstream(oldParam)

        on.exit({
            bpstop(BPPARAM)
            if (fallback)
                .RNGstream(oldParam) <- .RNGstream(BPPARAM)
        }, TRUE)
    }

    ## FUN
    FUN <- .composeTry(
        FUN, bplog(BPPARAM), bpstopOnError(BPPARAM),
        timeout=bptimeout(BPPARAM), exportglobals=bpexportglobals(BPPARAM),
        force.GC = bpforceGC(BPPARAM)
    )

    res <- bploop(
        manager, # dispatch
        FUN = FUN,
        BPPARAM = BPPARAM,
        ...
    )
    res
}

