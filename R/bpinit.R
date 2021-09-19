bpinit <- function(FUN, BPPARAM, ...){
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
        timeout=bptimeout(BPPARAM), exportglobals=bpexportglobals(BPPARAM)
    )

    cls <- structure(list(), class="iterate")
    res <- bploop(cls, # dispatch
                  FUN = FUN,
                  BPPARAM = BPPARAM,
                  ...)
    res
}


