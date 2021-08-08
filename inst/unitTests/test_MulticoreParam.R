test_MulticoreParam_progressbar <- function()
{
    if (.Platform$OS.type == "windows")
        return()

    checkIdentical(bptasks(MulticoreParam()), 0L)
    checkIdentical(bptasks(MulticoreParam(tasks = 0L, progressbar = TRUE)), 0L)
    checkIdentical(
        bptasks(MulticoreParam(progressbar = TRUE)),
        .Machine$integer.max
    )
}
