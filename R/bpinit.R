bpinit <- function(manager, FUN, BPPARAM, ...){
    fallback_PARAM <- FALSE
    if(!inherits(BPPARAM, "SerialParam")){
        if (!bpschedule(BPPARAM) || bpnworkers(BPPARAM) == 1L) {
            fallback_PARAM <- TRUE
            BPPARAM_old <- BPPARAM
            BPPARAM <- as(BPPARAM, "SerialParam")
        }
    }

    ## start / stop cluster
    if (!bpisup(BPPARAM)) {
        BPPARAM <- bpstart(BPPARAM)
        ## the fallback SerialParam must inherit the seed stream from BPPARAM
        if (fallback_PARAM && bpisup(BPPARAM_old))
            .RNGstream(BPPARAM) <- .RNGstream(BPPARAM_old)

        on.exit({
            bpstop(BPPARAM)
            if (fallback_PARAM)
                .RNGstream(BPPARAM_old) <- .RNGstream(BPPARAM)
        }, TRUE)
    }

    ## FUN
    FUN <- .composeTry(
        FUN, bplog(BPPARAM), bpstopOnError(BPPARAM),
        timeout=bptimeout(BPPARAM), exportglobals=bpexportglobals(BPPARAM),
        force.GC = bpforceGC(BPPARAM)
    )

    res <- bploop(manager, # dispatch
                  FUN = FUN,
                  BPPARAM = BPPARAM,
                  ...)
    res
}


