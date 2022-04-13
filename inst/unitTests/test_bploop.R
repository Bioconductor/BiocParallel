.lapplyReducer <- BiocParallel:::.lapplyReducer
.iterateReducer <- BiocParallel:::.iterateReducer

.reducer_value <- BiocParallel:::.reducer_value
.reducer_add <- BiocParallel:::.reducer_add
.reducer_ok <-  BiocParallel:::.reducer_ok
.reducer_complete <-  BiocParallel:::.reducer_complete

unevaluated <- BiocParallel:::.error_unevaluated()
notAvailable <- BiocParallel:::.error_not_available("HI")

## Normal reduce process
test_reducer_lapply_1 <- function() {
    r <- .lapplyReducer(10, NULL)
    result <- rep(list(unevaluated), 10)
    checkIdentical(result, .reducer_value(r))
    result[3:4] <- list(3,4)
    checkIdentical(result, { .reducer_add(r, 3:4, list(3,4)); .reducer_value(r) })
    result[1:2] <- list(1,2)
    checkIdentical(result, { .reducer_add(r, 1:2, list(1,2)); .reducer_value(r) })

    result[5:6] <- list(5,6)
    checkIdentical(result, { .reducer_add(r, 5:6, list(5,6)); .reducer_value(r) })

    checkTrue(.reducer_ok(r))
    checkTrue(!.reducer_complete(r))

    result[7:10] <- list(7,8,9,10)
    checkIdentical(result, { .reducer_add(r, 7:10, list(7,8,9,10)); .reducer_value(r) })

    checkTrue(.reducer_ok(r))
    checkTrue(.reducer_complete(r))
}

## REDO
test_reducer_lapply_2 <- function() {
    r <- .lapplyReducer(10, NULL)
    result <- rep(list(unevaluated), 10)
    checkIdentical(result, .reducer_value(r))
    result[1:4] <- list(1,2,3,4)
    checkIdentical(result, { .reducer_add(r, 1:4, list(1,2,3,4)); .reducer_value(r) })

    checkTrue(.reducer_ok(r))
    checkTrue(!.reducer_complete(r))

    values <- list(notAvailable,notAvailable,notAvailable,8,
                   notAvailable,notAvailable)
    result[5:10] <- values
    checkIdentical(result, { .reducer_add(r, 5:10, values); .reducer_value(r) })

    checkTrue(!.reducer_ok(r))
    checkTrue(.reducer_complete(r))

    ## REDO
    r2 <- .lapplyReducer(10, r)
    ## 5 errors/unfinished values
    checkIdentical(5L, r2$total)

    checkTrue(.reducer_ok(r2))
    checkTrue(!.reducer_complete(r2))

    result[9:10] <- list(9,10)
    checkIdentical(result, { .reducer_add(r2, 9:10, list(9,10)); .reducer_value(r2) })

    result[5:7] <- list(5,6,7)
    checkIdentical(result, { .reducer_add(r2, 5:7, list(5,6,7)); .reducer_value(r2) })

    checkTrue(.reducer_ok(r2))
    checkTrue(.reducer_complete(r2))

    ## REDO with new error
    r3 <- .lapplyReducer(10, r)
    result[5:7] <- list(5,6,notAvailable)
    .reducer_add(r3, 5:7, list(5,6,notAvailable))
    .reducer_add(r3, 9:10, list(9,10))
    checkIdentical(result, .reducer_value(r3))
}

## default reducer and reduce in order
test_reducer_iterate_1 <- function() {
    r <- .iterateReducer(reduce.in.order=TRUE,
                         reducer = NULL)

    checkTrue(.reducer_ok(r))
    ## The reducer has no idea about the length of the result
    checkTrue(.reducer_complete(r))

    checkIdentical(list(), .reducer_value(r))

    .reducer_add(r, 2, list(2))
    expect <- structure(list(NULL,2), .bperrors = list('1'=unevaluated))
    checkIdentical(expect, .reducer_value(r))

    checkTrue(.reducer_ok(r))
    ## The reducer knows at least the result 1 is missing
    checkTrue(!.reducer_complete(r))

    .reducer_add(r, 1, list(1))
    expect <- list(1,2)
    checkIdentical(expect, .reducer_value(r))

    .reducer_add(r, 3, list(3))
    expect <- list(1,2,3)
    checkIdentical(expect, .reducer_value(r))

    .reducer_add(r, 5, list(notAvailable))
    expect <- structure(
        list(1,2,3,NULL,NULL),
        .bperrors=list('4'=unevaluated,'5'=notAvailable)
    )
    checkIdentical(expect, .reducer_value(r))

    checkTrue(!.reducer_ok(r))
    checkTrue(!.reducer_complete(r))

    ## BPREDO
    r2 <- .iterateReducer(reducer = r)
    checkIdentical("5", names(r2$errors))

    checkTrue(!.reducer_ok(r2))
    checkTrue(!.reducer_complete(r2))

    .reducer_add(r2, 5, list(5))
    expect <- structure(
        list(1,2,3,NULL,5),
        .bperrors=list('4'=unevaluated)
    )
    checkIdentical(expect, .reducer_value(r2))

    checkTrue(.reducer_ok(r2))
    checkTrue(!.reducer_complete(r2))

    .reducer_add(r2, 4, list(4))
    expect <- list(1,2,3,4,5)
    checkIdentical(expect, .reducer_value(r2))

    checkTrue(.reducer_ok(r2))
    checkTrue(.reducer_complete(r2))

    .reducer_add(r2, 6, list(6))
    expect <- list(1,2,3,4,5,6)
    checkIdentical(expect, .reducer_value(r2))

    checkTrue(.reducer_ok(r2))
    checkTrue(.reducer_complete(r2))

    .reducer_add(r2, 7, list(notAvailable))
    expect <- structure(
        list(1,2,3,4,5,6,NULL),
        .bperrors=list('7'=notAvailable)
    )
    checkIdentical(expect, .reducer_value(r2))

    checkTrue(!.reducer_ok(r2))
    checkTrue(!.reducer_complete(r2))


    ## BPREDO 2
    r3 <- .iterateReducer(reducer = r2)
    checkIdentical("7", names(r3$errors))

    .reducer_add(r3, 7, list(7))
    expect <- list(1,2,3,4,5,6,7)
    checkIdentical(expect, .reducer_value(r3))

    checkTrue(.reducer_ok(r3))
    checkTrue(.reducer_complete(r3))
}

## customized reducer and reduce in order
test_reducer_iterate_2 <- function() {
    r <- .iterateReducer(`+`, init=0, reduce.in.order=TRUE,
                                         reducer = NULL)
    checkIdentical(0, .reducer_value(r))

    .reducer_add(r, 1, list(1))
    expect <- 1
    checkIdentical(expect, .reducer_value(r))

    .reducer_add(r, 3, list(3))
    expect <- structure(1, .bperrors = list('2' = unevaluated))
    checkIdentical(expect, .reducer_value(r))

    checkTrue(.reducer_ok(r))
    checkTrue(!.reducer_complete(r))

    .reducer_add(r, 2, list(2))
    expect <- 6
    checkIdentical(expect, .reducer_value(r))

    .reducer_add(r, 5, list(notAvailable))
    expect <- structure(6, .bperrors = list('4' = unevaluated, '5' = notAvailable))
    checkIdentical(expect, .reducer_value(r))

    checkTrue(!.reducer_ok(r))
    checkTrue(!.reducer_complete(r))

    ## BPREDO round1
    r2 <- .iterateReducer(reducer = r)
    checkIdentical("5", names(r2$errors))

    .reducer_add(r2, 5, list(5))
    expect <- structure(6, .bperrors = list('4' = unevaluated))
    checkIdentical(expect, .reducer_value(r2))

    .reducer_add(r2, 4, list(4))
    expect <- 15
    checkIdentical(expect, .reducer_value(r2))

    checkTrue(.reducer_ok(r2))
    checkTrue(.reducer_complete(r2))

    .reducer_add(r2, 6, list(notAvailable))
    expect <- structure(15, .bperrors = list('6' = notAvailable))
    checkIdentical(expect, .reducer_value(r2))

    checkTrue(!.reducer_ok(r2))
    checkTrue(!.reducer_complete(r2))

    ## BPREDO round2
    r3 <- .iterateReducer(reducer = r2)
    checkIdentical("6", names(r3$errors))

    .reducer_add(r3, 6, list(6))
    expect <- 21
    checkIdentical(expect, .reducer_value(r3))

    .reducer_add(r3, 7, list(7))
    expect <- 28
    checkIdentical(expect, .reducer_value(r3))

    checkTrue(.reducer_ok(r3))
    checkTrue(.reducer_complete(r3))
    checkTrue(all(sapply(as.list(r3$value.cache), is.null)))
}

## customized reducer and reduce not in order
test_reducer_iterate_3 <- function() {
    r <- .iterateReducer(`+`, init=0, reduce.in.order=FALSE,
                         reducer = NULL)
    checkIdentical(0, .reducer_value(r))

    .reducer_add(r, 1, list(1))
    expect <- 1
    checkIdentical(expect, .reducer_value(r))

    .reducer_add(r, 3, list(3))
    expect <- structure(4, .bperrors = list('2' = unevaluated))
    checkIdentical(expect, .reducer_value(r))

    checkTrue(.reducer_ok(r))
    checkTrue(!.reducer_complete(r))

    .reducer_add(r, 2, list(2))
    expect <- 6
    checkIdentical(expect, .reducer_value(r))

    .reducer_add(r, 5, list(notAvailable))
    expect <- structure(6, .bperrors = list('4' = unevaluated, '5' = notAvailable))
    checkIdentical(expect, .reducer_value(r))

    checkTrue(!.reducer_ok(r))
    checkTrue(!.reducer_complete(r))

    ## BPREDO round1
    r2 <- .iterateReducer(reducer = r)
    checkIdentical("5", names(r2$errors))

    .reducer_add(r2, 5, list(5))
    expect <- structure(11, .bperrors = list('4' = unevaluated))
    checkIdentical(expect, .reducer_value(r2))

    .reducer_add(r2, 4, list(4))
    expect <- 15
    checkIdentical(expect, .reducer_value(r2))

    checkTrue(.reducer_ok(r2))
    checkTrue(.reducer_complete(r2))

    .reducer_add(r2, 6, list(notAvailable))
    expect <- structure(15, .bperrors = list('6' = notAvailable))
    checkIdentical(expect, .reducer_value(r2))

    checkTrue(!.reducer_ok(r2))
    checkTrue(!.reducer_complete(r2))

    ## BPREDO round2
    r3 <- .iterateReducer(reducer = r2)
    checkIdentical("6", names(r3$errors))

    .reducer_add(r3, 6, list(6))
    expect <- 21
    checkIdentical(expect, .reducer_value(r3))

    .reducer_add(r3, 7, list(7))
    expect <- 28
    checkIdentical(expect, .reducer_value(r3))

    checkTrue(.reducer_ok(r3))
    checkTrue(.reducer_complete(r3))
    checkTrue(all(sapply(as.list(r3$value.cache), is.null)))
}

## Test for a marginal case where the result is NULL
## and contains error
test_reducer_iterate_4 <- function() {
    r <- .iterateReducer(function(x,y)NULL, init=NULL,
                         reduce.in.order=FALSE,
                         reducer = NULL)

    checkIdentical(NULL, .reducer_value(r))

    .reducer_add(r, 1, list(1))
    expect <- NULL
    checkIdentical(expect, .reducer_value(r))

    .reducer_add(r, 2, list(notAvailable))
    expect <- structure(list(),.bperrors=list('2'=notAvailable))
    checkIdentical(expect, .reducer_value(r))
}

