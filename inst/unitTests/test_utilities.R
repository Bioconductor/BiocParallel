test_splitIndicies <- function()
{
    .splitIndices <- BiocParallel:::.splitIndices

    checkIdentical(integer(), .splitIndices(0, 0))
    checkIdentical(integer(), .splitIndices(0, 1))
    checkIdentical(integer(), .splitIndices(0, 2))

    checkIdentical(1:4, .splitIndices(4, 0))
    checkIdentical(1:4, .splitIndices(4, 1))

    checkIdentical(list(1:4, 5:7), .splitIndices(7, 2))
}
