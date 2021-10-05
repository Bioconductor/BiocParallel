.bpinit <-
    function(manager, FUN, BPPARAM, BPREDO = list(), ...)
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

    ## record the initial seed used by the BPPARAM
    BPREDOSEED <- .RNGstream(BPPARAM)

    ## If BPREDO present and contains a seed, we need to
    ## 1. use the seed from BPREDO
    ## 2. recover the old seed after computing the result
    if (length(BPREDO) && !is.null(attr(BPREDO, "BPREDOSEED"))) {
        .RNGstream(BPPARAM) <- attr(BPREDO, "BPREDOSEED")
        on.exit({
            .RNGstream(BPPARAM) <- BPREDOSEED
        }, TRUE, after = FALSE)
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

    if (length(BPREDO)) {
        redo_idx <- which(!bpok(BPREDO))
        if (length(redo_idx))
            BPREDO[redo_idx] <- res
        res <- BPREDO
    }

    if (!all(bpok(res))) {
        ## attach the seed only when no
        ## BPREDO presents
        if (!length(BPREDO))
            attr(res, "BPREDOSEED") <- BPREDOSEED
    } else {
        attr(res, "BPREDOSEED") <- NULL
    }

    res
}

