message("Testing SerialParam")

test_SerialParam_bpnworkers <- function() {
    checkIdentical(1L, bpnworkers(SerialParam()))
    checkIdentical(1L, bpnworkers(bpstart(SerialParam())))
    checkIdentical(1L, bpnworkers(bpstop(bpstart(SerialParam()))))
}

test_SerialParam_bpbackend <- function() {
    checkIdentical(NULL, bpbackend(SerialParam()))
    checkTrue(is(bpbackend(bpstart(SerialParam())), "SerialBackend"))
    checkIdentical(NULL, bpbackend(bpstop(bpstart(SerialParam()))))
}

test_SerialParam_bpforceGC <- function() {
    checkIdentical(FALSE, bpforceGC(SerialParam()))
    checkIdentical(FALSE, bpforceGC(SerialParam(force.GC = FALSE)))
    checkIdentical(TRUE, bpforceGC(SerialParam(force.GC = TRUE)))
    checkException(SerialParam(force.GC = NA), silent = TRUE)
    checkException(SerialParam(force.GC = 1:2), silent = TRUE)
}

test_SerialParam_bpisup_start_stop <- function() {
    param <- SerialParam()
    checkIdentical(FALSE, bpisup(param)) # not always up
    param <- bpstart(param)
    checkIdentical(TRUE, bpisup(param))
    param <- bpstop(param)
    checkIdentical(FALSE, bpisup(param))
}
