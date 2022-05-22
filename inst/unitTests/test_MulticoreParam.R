message("Testing MulticoreParam")

test_MulticoreParam_progressbar <- function()
{
    if (.Platform$OS.type == "windows")
        return()

    checkIdentical(bptasks(MulticoreParam()), 0L)
    checkIdentical(bptasks(MulticoreParam(tasks = 0L, progressbar = TRUE)), 0L)
    checkIdentical(
        bptasks(MulticoreParam(progressbar = TRUE)),
        BiocParallel:::TASKS_MAXIMUM
    )
}

test_MulticoreParam_bpforceGC <- function() {
    if (.Platform$OS.type == "windows")
        return()

    checkIdentical(TRUE, bpforceGC(MulticoreParam()))
    checkIdentical(FALSE, bpforceGC(MulticoreParam(force.GC = FALSE)))
    checkIdentical(TRUE, bpforceGC(MulticoreParam(force.GC = TRUE)))
    checkException(MulticoreParam(force.GC = NA), silent = TRUE)
    checkException(MulticoreParam(force.GC = 1:2), silent = TRUE)
}
