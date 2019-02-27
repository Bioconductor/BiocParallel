test_SerialParam_bpnworkers <- function() {
    checkIdentical(1L, bpnworkers(SerialParam()))
    checkIdentical(1L, bpnworkers(bpstart(SerialParam())))
    checkIdentical(1L, bpnworkers(bpstop(bpstart(SerialParam()))))
}

test_SerialParam_bpbackend <- function() {
    checkIdentical(list(FALSE), bpbackend(SerialParam()))
    checkIdentical(list(TRUE), bpbackend(bpstart(SerialParam())))
    checkIdentical(list(FALSE), bpbackend(bpstop(bpstart(SerialParam()))))
}

test_SerialParam_bpisup_start_stop <- function() {
    param <- SerialParam()
    checkIdentical(FALSE, bpisup(param)) # not always up
    param <- bpstart(param)
    checkIdentical(TRUE, bpisup(param))
    param <- bpstop(param)
    checkIdentical(FALSE, bpisup(param))
}
