test_rng_seeds_by_task <- function()
{
    .rng_get_generator <- BiocParallel:::.rng_get_generator
    .rng_seeds_by_task <- BiocParallel:::.rng_seeds_by_task

    target <- .rng_get_generator()
    obs <- .rng_seeds_by_task(bpstart(SerialParam()), !logical(5), c(2, 3))
    checkIdentical(target, .rng_get_generator(), ".rng_seeds_by_task()")
}

test_rng_geometry <- function()
{
    .rng_get_generator <- BiocParallel:::.rng_get_generator
    .rng_reset_generator <- BiocParallel:::.rng_reset_generator
    .rng_seeds_by_task <- BiocParallel:::.rng_seeds_by_task

    state <- .rng_get_generator()
    on.exit(.rng_reset_generator(state$kind, state$seed))

    param <- SerialParam(RNGseed = 123)

    ## 0 tasks
    target <- .rng_seeds_by_task(bpstart(param), logical(), integer())
    bpstop(param)
    checkIdentical(list(), target)

    ## five independent streams
    target <- .rng_seeds_by_task(bpstart(param), !logical(5), rep(1, 5))
    bpstop(param)

    ## 2 tasks of 2 + 3 jobs
    obs <- .rng_seeds_by_task(bpstart(param), !logical(5), c(2, 3))
    bpstop(param)
    checkIdentical(obs, target[c(1, 3)])

    ## 2 tasks of 3 + 2 jobs
    obs <- .rng_seeds_by_task(bpstart(param), !logical(5), c(3, 2))
    bpstop(param)
    checkIdentical(obs, target[c(1, 4)])

    ## 0 tasks
    obs <- .rng_seeds_by_task(bpstart(param), !logical(5), c(2, 0, 3))
    bpstop(param)
    checkIdentical(obs, list(target[[1]], integer(), target[[3]]))

    checkIdentical(state, .rng_get_generator())
}

test_rng_fun_advances_generator <- function()
{
    .rng_get_generator <- BiocParallel:::.rng_get_generator
    .rng_reset_generator <- BiocParallel:::.rng_reset_generator
    .rng_seeds_by_task <- BiocParallel:::.rng_seeds_by_task
    .rng_job_fun_factory <- BiocParallel:::.rng_job_fun_factory

    state <- .rng_get_generator()
    on.exit(.rng_reset_generator(state$kind, state$seed))

    SEED <- .rng_seeds_by_task(bpstart(SerialParam()), TRUE, 1L)[[1]]
    checkIdentical(
        ## independently invoked with same seed --> same result
        .rng_job_fun_factory(function(i) rnorm(i), SEED)(2),
        .rng_job_fun_factory(function(i) rnorm(i), SEED)(2)
    )

    SEED <- .rng_seeds_by_task(bpstart(SerialParam()), TRUE, 1L)[[1]]
    FUN <- .rng_job_fun_factory(function(i) rnorm(i), SEED)
    target <- FUN(2) # two numbers from same stream

    FUN <- .rng_job_fun_factory(function(i) rnorm(i), SEED)
    obs <- c(FUN(1), FUN(1)) # two numbers from separate streams
    checkIdentical(obs[[1]], target[[1]])
    checkTrue(obs[[2]] != target[[2]])
}

test_rng_lapply <- function()
{
    .rng_get_generator <- BiocParallel:::.rng_get_generator
    .rng_reset_generator <- BiocParallel:::.rng_reset_generator
    .rng_seeds_by_task <- BiocParallel:::.rng_seeds_by_task
    .rng_lapply <- BiocParallel:::.rng_lapply
    .rng_next_substream <- BiocParallel:::.rng_next_substream

    state <- .rng_get_generator()
    on.exit(.rng_reset_generator(state$kind, state$seed))

    SEED <- .rng_seeds_by_task(bpstart(SerialParam()), TRUE, 1L)[[1]]
    checkIdentical(
        ## same sequence of random number streams
        .rng_lapply(1:2, function(i) rnorm(1), BPRNGSEED = SEED),
        .rng_lapply(1:2, function(i) rnorm(1), BPRNGSEED = SEED)
    )
    checkIdentical(state, .rng_get_generator())

    SEED1 <- .rng_seeds_by_task(bpstart(SerialParam()), TRUE, 1L)[[1]]
    SEED2 <- .rng_next_substream(SEED1)
    target <- .rng_lapply(1:2, function(i) rnorm(2), BPRNGSEED = SEED1)
    obs <- c(
        .rng_lapply(1, function(i) rnorm(2), BPRNGSEED = SEED1),
        .rng_lapply(1, function(i) rnorm(2), BPRNGSEED = SEED2)
    )
    checkIdentical(target, obs)
}

test_rng_bplapply <- function()
{
    .rng_get_generator <- BiocParallel:::.rng_get_generator
    .rng_reset_generator <- BiocParallel:::.rng_reset_generator

    state <- .rng_get_generator()
    on.exit(.rng_reset_generator(state$kind, state$seed))

    p1 <- SerialParam(RNGseed = 123)
    p2 <- SnowParam(3, RNGseed = 123)
    p3 <- SnowParam(5, RNGseed = 123)
    FUN <- function(i) rnorm(2)

    ## SerialParam / SnowParam same results
    target <- bplapply(1:11, FUN, BPPARAM = p1)
    checkIdentical(bplapply(1:11, FUN, BPPARAM = p2), target)

    ## SerialParam / SnowParam same results, different number of workers
    checkIdentical(bplapply(1:11, FUN, BPPARAM = p3), target)

    if (identical(.Platform$OS.type, "unix")) {
        ## SerialParam / MulticoreParam same results
        p4 <- MulticoreParam(5, RNGseed = 123)
        checkIdentical(bplapply(1:11, FUN, BPPARAM = p4), target)
    }
}

test_rng_bpiterate <- function()
{
    .rng_get_generator <- BiocParallel:::.rng_get_generator
    .rng_reset_generator <- BiocParallel:::.rng_reset_generator

    state <- .rng_get_generator()
    on.exit(.rng_reset_generator(state$kind, state$seed))

    FUN <- function(i) rnorm(2)

    ITER_factory <- function() {
        x <- 1:11
        i <- 0L
        function() {
            i <<- i + 1L
            if (i > length(x))
                return(NULL)
            x[[i]]
        }
    }

    p1 <- SerialParam(RNGseed = 123)
    p2 <- SnowParam(3, RNGseed = 123)
    p3 <- SnowParam(5, RNGseed = 123)

    target <- bplapply(1:11, FUN, BPPARAM = p1)
    checkIdentical(target, bpiterate(ITER_factory(), FUN, BPPARAM = p1), "p1")
    checkIdentical(target, bpiterate(ITER_factory(), FUN, BPPARAM = p2), "p2")
    checkIdentical(target, bpiterate(ITER_factory(), FUN, BPPARAM = p3), "p3")

    if (identical(.Platform$OS.type, "unix")) {
        ## SerialParam / MulticoreParam same results
        p4 <- MulticoreParam(5, RNGseed = 123)
        checkIdentical(
            target, bpiterate(ITER_factory(), FUN, BPPARAM = p4),
            "p4"
        )
    }
}

test_rng_bpstart <- function()
{
    .rng_get_generator <- BiocParallel:::.rng_get_generator
    .rng_reset_generator <- BiocParallel:::.rng_reset_generator
    state <- .rng_get_generator()

    FUN <- function(i) rnorm(2)

    ITER_factory <- function() {
        x <- 1:11
        i <- 0L
        function() {
            i <<- i + 1L
            if (i > length(x))
                return(NULL)
            x[[i]]
        }
    }

    ## bplapply
    p0 <- bpstart(SerialParam()) # random seed
    result1 <- unlist(bplapply(1:11, FUN, BPPARAM = p0))
    result2 <- unlist(bplapply(1:11, FUN, BPPARAM = p0))
    checkTrue(!any(result1 %in% result2))

    bpstart(bpstop(p0)) # different random seed
    result3 <- unlist(bplapply(1:11, FUN, BPPARAM = p0))
    checkTrue(!any(result3 %in% result1))

    p0 <- bpstart(SerialParam(RNGseed = 123)) # set seed
    result1 <- unlist(bplapply(1:11, FUN, BPPARAM = p0))
    result2 <- unlist(bplapply(1:11, FUN, BPPARAM = p0))
    checkTrue(!any(result1 %in% result2))

    bpstart(bpstop(p0)) # reset seed, same stream
    result3 <- unlist(bplapply(1:11, FUN, BPPARAM = p0))
    result4 <- unlist(bplapply(1:11, FUN, BPPARAM = p0))
    checkIdentical(result3, result1)
    checkIdentical(result4, result2)

    ## bpiterate
    p0 <- bpstart(SerialParam()) # random seed
    result1 <- unlist(bpiterate(ITER_factory(), FUN, BPPARAM = p0))
    result2 <- unlist(bpiterate(ITER_factory(), FUN, BPPARAM = p0))
    checkTrue(!any(result1 %in% result2))

    bpstart(bpstop(p0)) # different random seed
    result3 <- unlist(bpiterate(ITER_factory(), FUN, BPPARAM = p0))
    checkTrue(!any(result3 %in% result1))

    p0 <- bpstart(SerialParam(RNGseed = 123)) # set seed
    result1 <- unlist(bpiterate(ITER_factory(), FUN, BPPARAM = p0))
    result2 <- unlist(bpiterate(ITER_factory(), FUN, BPPARAM = p0))
    checkTrue(!any(result1 %in% result2))

    bpstart(bpstop(p0)) # reset seed, same stream
    result3 <- unlist(bpiterate(ITER_factory(), FUN, BPPARAM = p0))
    result4 <- unlist(bpiterate(ITER_factory(), FUN, BPPARAM = p0))
    checkIdentical(result3, result1)
    checkIdentical(result4, result2)
}

.test_rng_bpstart_iterates_rng_seed <- function(param) {
    set.seed(123L)
    target <- runif(2L)[2L]

    ## bpstart() should increment the random number seed by exactly one call
    set.seed(123L)
    bpstart(param)
    checkIdentical(target, runif(1L))
    bpstop(param)

    set.seed(123)
    result <- bplapply(1:3, runif, BPPARAM = param)
    checkIdentical(target, runif(1L))
    checkTrue(!identical(result, bplapply(1:3, runif, BPPARAM = param)))

    set.seed(123)
    checkIdentical(result, bplapply(1:3, runif, BPPARAM = param))
    checkIdentical(target, runif(1L))
}

test_rng_bpstart_iterates_rng_seed <- function() {
    .rng_get_generator <- BiocParallel:::.rng_get_generator
    .rng_reset_generator <- BiocParallel:::.rng_reset_generator

    state <- .rng_get_generator()
    on.exit(.rng_reset_generator(state$kind, state$seed))

    .test_rng_bpstart_iterates_rng_seed(SerialParam())
    .test_rng_bpstart_iterates_rng_seed(SnowParam(2))
    if (identical(.Platform$OS.type, "unix"))
        .test_rng_bpstart_iterates_rng_seed(MulticoreParam(2))
}

.test_rng_global_and_RNGseed_identical <- function(param_fun) {
    set.seed(123)
    target <- bplapply(1:3, runif, BPPARAM = param_fun())
    current <- bplapply(1:3, runif, BPPARAM = param_fun(RNGseed = 123))
    checkIdentical(target, current)

    set.seed(123)
    current <- bplapply(1:3, runif, BPPARAM = param_fun(RNGseed = 124))
    checkTrue(!identical(target, current))
}

test_rng_global_and_RNGseed_identical <- function() {
    .rng_get_generator <- BiocParallel:::.rng_get_generator
    .rng_reset_generator <- BiocParallel:::.rng_reset_generator

    state <- .rng_get_generator()
    on.exit(.rng_reset_generator(state$kind, state$seed))

    .test_rng_global_and_RNGseed_identical(SerialParam)
    .test_rng_global_and_RNGseed_identical(SnowParam)
    if (identical(.Platform$OS.type, "unix"))
        .test_rng_global_and_RNGseed_identical(MulticoreParam)
}

.test_rng_bpredo_impl <- function(param) {
    FUN <- function(i) rnorm(1)
    target <- unlist(bplapply(1:11, FUN, BPPARAM = param))

    FUN0 <- function(i) {
        if (identical(i, 7L)) {
            stop("i == 7")
        } else rnorm(1)
    }
    result <- bptry(bplapply(1:11, FUN0, BPPARAM = param))
    checkIdentical(unlist(result[-7]), target[-7])
    checkTrue(inherits(result[[7]], "remote_error"))

    FUN1 <- function(i) {
        if (identical(i, 7L)) {
            ## the random number stream should be in the same state as the
            ## first time through the loop, and rnorm(1) should return
            ## same result as FUN
            rnorm(1)
        } else {
            ## if this branch is used, then we are incorrectly updating
            ## already calculated elements -- '0' in the output would
            ## indicate this error
            0
        }
    }
    result <- unlist(bplapply(1:11, FUN1, BPREDO = result, BPPARAM = param))
    checkIdentical(result, target)
}

test_rng_bpredo <- function()
{
    .rng_get_generator <- BiocParallel:::.rng_get_generator
    .rng_reset_generator <- BiocParallel:::.rng_reset_generator

    state <- .rng_get_generator()
    on.exit(.rng_reset_generator(state$kind, state$seed))

    param <- SerialParam(RNGseed = 123, stop.on.error = FALSE)
    .test_rng_bpredo_impl(param)

    if (identical(.Platform$OS.type, "unix")) {
        param <- MulticoreParam(3, RNGseed = 123, stop.on.error = FALSE)
        .test_rng_bpredo_impl(param)
    }
}
