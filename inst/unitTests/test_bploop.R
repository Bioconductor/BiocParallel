test_reducer_1 <- function() {
    r <- BiocParallel:::.reducer()
    checkIdentical(list(), r$value())
    checkIdentical(list(1), { r$add(1, list(1)); r$value() })
    checkIdentical(list(1, 2), { r$add(2, list(2)); r$value() })
    checkIdentical(list(1, 2, 4), { r$add(4, list(4)); r$value() })
    checkIdentical(list(1, 2, 3, 4), { r$add(3, list(3)); r$value() })
}

test_reducer_2 <- function() {
    r <- BiocParallel:::.reducer(c)
    checkIdentical(list(), r$value())
    checkIdentical(c(1), { r$add(1, list(1)); r$value() })
    checkIdentical(c(1, 2), { r$add(2, list(2)); r$value() })
    checkIdentical(c(1, 2, 4), { r$add(4, list(4)); r$value() })
    checkIdentical(c(1, 2, 4, 3), { r$add(3, list(3)); r$value() })
}

test_reducer_3 <- function() {
    r <- BiocParallel:::.reducer(`+`, init=0)
    checkIdentical(0, r$value())
    checkIdentical(1, { r$add(1, list(1)); r$value() })
    checkIdentical(3, { r$add(2, list(2)); r$value() })
    checkIdentical(7, { r$add(4, list(4)); r$value() })
}

test_reducer_4 <- function() {
    r <- BiocParallel:::.reducer(c, reduce.in.order=TRUE)
    checkIdentical(list(), r$value())
    checkIdentical(1, { r$add(1, list(1)); r$value() })
    checkIdentical(c(1, 2), { r$add(2, list(2)); r$value() })
    checkIdentical(TRUE, r$isComplete())
    checkIdentical(c(1, 2), { r$add(4, list(4)); r$value() })
    checkIdentical(FALSE, r$isComplete())
    checkIdentical(c(1, 2, 3, 4), { r$add(3, list(3)); r$value() })
    checkIdentical(TRUE, r$isComplete())
}

test_reducer_5 <- function() {
    r <- BiocParallel:::.reducer(`+`, init=0, reduce.in.order=TRUE)
    checkIdentical(0, r$value())
    checkIdentical(1, { r$add(1, list(1)); r$value() })
    checkIdentical(3, { r$add(2, list(2)); r$value() })
    checkIdentical(TRUE, r$isComplete())
    checkIdentical(3, { r$add(4, list(4)); r$value() })
    checkIdentical(FALSE, r$isComplete())
    checkIdentical(10, { r$add(3, list(3)); r$value() })
    checkIdentical(TRUE, r$isComplete())
}

test_bploop_lapply_iter  <- function() {
    .bploop_lapply_iter <- BiocParallel:::.bploop_lapply_iter
    .bploop_rng_iter <- BiocParallel:::.bploop_rng_iter

    X <- 1:10
    redo_index <- c(1:2,6:8)
    iter <- .bploop_lapply_iter(X, redo_index, 10)
    checkIdentical(iter(), 1:2)
    checkIdentical(iter(), .bploop_rng_iter(3L))
    checkIdentical(iter(), 6:8)
    checkIdentical(iter(), list(NULL))
    checkIdentical(iter(), list(NULL))

    iter <- .bploop_lapply_iter(X, redo_index, 2)
    checkIdentical(iter(), 1:2)
    checkIdentical(iter(), .bploop_rng_iter(3L))
    checkIdentical(iter(), 6:7)
    checkIdentical(iter(), 8L)
    checkIdentical(iter(), list(NULL))
    checkIdentical(iter(), list(NULL))

    redo_index <- 6:8
    iter <- .bploop_lapply_iter(X, redo_index, 1)
    checkIdentical(iter(), .bploop_rng_iter(5L))
    checkIdentical(iter(), 6L)
    checkIdentical(iter(), 7L)
    checkIdentical(iter(), 8L)
    checkIdentical(iter(), list(NULL))
    checkIdentical(iter(), list(NULL))
}
