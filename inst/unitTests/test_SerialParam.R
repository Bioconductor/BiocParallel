test_SerialParam_bpisup_start_stop <- function() {
    param <- SerialParam()
    checkIdentical(FALSE, bpisup(param)) # always up
    param <- bpstart(param)
    checkIdentical(TRUE, bpisup(param))
    param <- bpstop(param)
    checkIdentical(FALSE, bpisup(param))
}

test_SerialParam_bpbackend <- function() {
    checkIdentical(list(), bpbackend(SerialParam()))
}
