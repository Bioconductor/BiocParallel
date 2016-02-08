test_reducer_1 <- function() {
    r <- BiocParallel:::.reducer()
    checkIdentical(list(), r$value())
    checkIdentical(list(1), { r$add(1, 1); r$value() })
    checkIdentical(list(1, 2), { r$add(2, 2); r$value() })
    checkIdentical(list(1, 2, 4), { r$add(4, 4); r$value() })
    checkIdentical(list(1, 2, 3, 4), { r$add(3, 3); r$value() })
}

test_reducer_2 <- function() {
    r <- BiocParallel:::.reducer(c)
    checkIdentical(list(), r$value())
    checkIdentical(c(1), { r$add(1, 1); r$value() })
    checkIdentical(c(1, 2), { r$add(2, 2); r$value() })
    checkIdentical(c(1, 2, 4), { r$add(4, 4); r$value() })
    checkIdentical(c(1, 2, 4, 3), { r$add(3, 3); r$value() })
}

test_reducer_3 <- function() {
    r <- BiocParallel:::.reducer(`+`, init=0)
    checkIdentical(0, r$value())
    checkIdentical(1, { r$add(1, 1); r$value() })
    checkIdentical(3, { r$add(2, 2); r$value() })
    checkIdentical(7, { r$add(4, 4); r$value() })
}

test_reducer_4 <- function() {
    r <- BiocParallel:::.reducer(c, reduce.in.order=TRUE)
    checkIdentical(list(), r$value())
    checkIdentical(1, { r$add(1, 1); r$value() })
    checkIdentical(c(1, 2), { r$add(2, 2); r$value() })
    checkIdentical(TRUE, r$isComplete())
    checkIdentical(c(1, 2), { r$add(4, 4); r$value() })
    checkIdentical(FALSE, r$isComplete())
    checkIdentical(c(1, 2, 3, 4), { r$add(3, 3); r$value() })
    checkIdentical(TRUE, r$isComplete())
}

test_reducer_5 <- function() {
    r <- BiocParallel:::.reducer(`+`, init=0, reduce.in.order=TRUE)
    checkIdentical(0, r$value())
    checkIdentical(1, { r$add(1, 1); r$value() })
    checkIdentical(3, { r$add(2, 2); r$value() })
    checkIdentical(TRUE, r$isComplete())
    checkIdentical(3, { r$add(4, 4); r$value() })
    checkIdentical(FALSE, r$isComplete())
    checkIdentical(10, { r$add(3, 3); r$value() })
    checkIdentical(TRUE, r$isComplete())
}
