test_rng_state_restored_after_evaluation <- function()
{
    .rng_get_generator <- BiocParallel:::.rng_get_generator
    .rng_seeds_by_task <- BiocParallel:::.rng_seeds_by_task

    target <- .rng_get_generator()
    obs <- .rng_seeds_by_task(SnowParam(2), c(2, 3))
    checkIdentical(target, .rng_get_generator(), ".rng_seeds_by_task()")

    bpstop(bpstart(SerialParam()))
    checkIdentical(target, .rng_get_generator(), "SerialParam()")
    bpstop(bpstart(SerialParam(RNGseed = 123)))
    checkIdentical(target, .rng_get_generator(), "SerialParam(RNGseed=)")

    bpstop(bpstart(SnowParam(2)))
    checkIdentical(target, .rng_get_generator(), "SnowParam()")
    bpstop(bpstart(SnowParam(2, RNGseed = 123)))
    checkIdentical(target, .rng_get_generator(), "SnowParam(RNGseed=)")

    if (identical(.Platform$OS.type, "unix")) {
        bpstop(bpstart(MulticoreParam(2)))
        checkIdentical(target, .rng_get_generator(), "MulticoreParam()")
        bpstop(bpstart(MulticoreParam(2, RNGseed = 123)))
        checkIdentical(target, .rng_get_generator(), "MulticoreParam(RNGseed=)")
    }
}

test_rng_geometry <- function()
{
    .rng_get_generator <- BiocParallel:::.rng_get_generator
    .rng_reset_generator <- BiocParallel:::.rng_reset_generator
    .rng_seeds_by_task <- BiocParallel:::.rng_seeds_by_task

    state <- .rng_get_generator()
    on.exit(.rng_reset_generator(state$kind, state$seed))

    param <- SnowParam(2, RNGseed = 123)

    ## five independent streams
    target <- .rng_seeds_by_task(param, rep(1, 5))

    ## 2 tasks of 2 + 3 jobs
    obs <- .rng_seeds_by_task(param, c(2, 3))
    checkIdentical(obs, target[c(1, 3)])

    ## 2 tasks of 3 + 2 jobs
    obs <- .rng_seeds_by_task(param, c(3, 2))
    checkIdentical(obs, target[c(1, 4)])

    ## 0 tasks
    obs <- .rng_seeds_by_task(param, c(2, 0, 3))
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

    SEED <- .rng_seeds_by_task(SnowParam(2), 1L)[[1]]
    checkIdentical(
        ## independently invoked with same seed --> same result
        .rng_job_fun_factory(function(i) rnorm(i), SEED)(2),
        .rng_job_fun_factory(function(i) rnorm(i), SEED)(2)
    )

    SEED <- .rng_seeds_by_task(SnowParam(2), 1L)[[1]]
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
    .rng_next_stream <- BiocParallel:::.rng_next_stream

    state <- .rng_get_generator()
    on.exit(.rng_reset_generator(state$kind, state$seed))

    SEED <- .rng_seeds_by_task(SnowParam(2), 1L)[[1]]
    checkIdentical(
        ## same sequence of random number streams
        .rng_lapply(1:2, function(i) rnorm(1), BPRNGSEED = SEED),
        .rng_lapply(1:2, function(i) rnorm(1), BPRNGSEED = SEED)
    )
    checkIdentical(state, .rng_get_generator())

    SEED1 <- .rng_seeds_by_task(SnowParam(2), 1L)[[1]]
    SEED2 <- .rng_next_stream(SEED1)
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

    checkIdentical(state, .rng_get_generator())
}

test_rng_bpiterate <- function()
{
    .rng_get_generator <- BiocParallel:::.rng_get_generator
    .rng_reset_generator <- BiocParallel:::.rng_reset_generator

    state <- .rng_get_generator()
    on.exit(.rng_reset_generator(state$kind, state$seed))

    p1 <- SerialParam(RNGseed = 123)
    p2 <- SnowParam(3, RNGseed = 123)
    p3 <- SnowParam(5, RNGseed = 123)
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

    checkIdentical(state, .rng_get_generator())
}
