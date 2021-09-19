bpinit <- function(manager, FUN, BPPARAM, ...){
    if(!inherits(BPPARAM, "SerialParam")){
        if (!bpschedule(BPPARAM) || bpnworkers(BPPARAM) == 1L) {
            BPPARAM <- as(BPPARAM, "SerialParam")
        }
    }

    ## start / stop cluster
    if (!bpisup(BPPARAM)) {
        BPPARAM <- bpstart(BPPARAM)
        on.exit(bpstop(BPPARAM), TRUE)
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


