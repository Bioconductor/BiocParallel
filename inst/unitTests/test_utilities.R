test_splitIndicies <- function()
{
    .splitIndices <- BiocParallel:::.splitIndices

    checkIdentical(list(), .splitIndices(0, 0))
    checkIdentical(list(), .splitIndices(0, 1))
    checkIdentical(list(), .splitIndices(0, 2))

    checkIdentical(list(1:4), .splitIndices(4, 0))
    checkIdentical(list(1:4), .splitIndices(4, 1))
    checkIdentical(list(1:2, 3:4), .splitIndices(4, 2))
    checkIdentical(as.list(1:4), .splitIndices(4, 4))
    checkIdentical(as.list(1:4), .splitIndices(4, 8))

    checkIdentical(list(1:4, 5:7), .splitIndices(7, 2))
}

test_splitX <- function()
{
    .splitX <- BiocParallel:::.splitX

    checkIdentical(list(), .splitX(character(), 0, 0))
    checkIdentical(list(), .splitX(character(), 1, 0))
    checkIdentical(list(), .splitX(character(), 0, 1))
    checkIdentical(list(), .splitX(character(), 1, 1))

    X <- LETTERS[1:4]
    checkIdentical(list(X),              .splitX(X, 0, 0))
    checkIdentical(list(X),              .splitX(X, 1, 0))
    checkIdentical(list(X[1:2], X[3:4]), .splitX(X, 2, 0))
    checkIdentical(as.list(X),           .splitX(X, 4, 0))
    checkIdentical(as.list(X),           .splitX(X, 8, 0))

    checkIdentical(list(X[1:2], X[3:4]), .splitX(X, 2, 0))
    checkIdentical(list(X), .splitX(X, 2, 1))
    checkIdentical(list(X[1:2], X[3:4]),     .splitX(X, 2, 2))
    checkIdentical(list(X[1], X[2:3], X[4]), .splitX(X, 2, 3))
    checkIdentical(as.list(X),               .splitX(X, 2, 4))
}

test_redo_index <- function() {
    .redo_index <- BiocParallel:::.redo_index
    err <- BiocParallel:::.error("")
    checkIdentical(logical(), .redo_index(list(), list()))
    checkIdentical(TRUE, .redo_index(list(1), list(err)))
    checkIdentical(c(FALSE, TRUE), .redo_index(list(1, "2"), list(1, err)))
    ## all need recalculating
    checkIdentical(c(TRUE, TRUE), .redo_index(list("1", "2"), list(err, err)))
    ## X can be a vector
    checkIdentical(c(FALSE, TRUE), .redo_index(1:2, list(1, err)))
    ## lengths differ
    checkException(.redo_index(list(1, 2), list(err)), silent=TRUE)
    ## no previous error
    checkException(.redo_index(list(1, 2), list(1, 2)), silent=TRUE)
}
