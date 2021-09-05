## .rng_get_generator(): get the current generator kind and seed
.rng_get_generator <-
    function()
{
    seed <-
        if (exists(".Random.seed", envir = .GlobalEnv, inherits = FALSE)) {
            get(".Random.seed", envir = .GlobalEnv, inherits = FALSE)
        } else NULL
    kind <- RNGkind()

    list(kind = kind, seed = seed)
}
## .rng_reset_generator(): reset the generator to a state returned by
## .rng_get_generator()
.rng_reset_generator <-
    function(kind, seed)
{
    ## Setting RNGkind() changes the seed, so restore the original
    ## seed after restoring the kind
    RNGkind(kind[[1]])
    if (is.null(seed)) {
        rm(.Random.seed, envir = .GlobalEnv)
    } else {
        assign(".Random.seed", seed, envir = .GlobalEnv)
    }

    list(kind = kind, seed = seed)
}

## .rng_init_stream(): initialize the generator to a new kind,
## optionally using `set.seed()` to set the seed for the the
## generator.
.rng_init_stream <-
    function(seed)
{
    state <- .rng_get_generator()
    on.exit(.rng_reset_generator(state$kind, state$seed))

    ## coerces seed to appropriate format for RNGkind; NULL seed (from
    ## bpRNGseed()) uses the global random number stream.
    if (!is.null(seed))
        set.seed(seed)

    ## change kind
    kind <- "L'Ecuyer-CMRG"
    RNGkind(kind)

    ## .Random.seed always exists after RNGkind()
    get(".Random.seed", envir = .GlobalEnv, inherits = FALSE)
}

## .rng_next_stream(): return the next stream for a parallel job
.rng_next_stream <-
    function(seed)
{
    ## `nextRNGStream()` does not require that the current stream is
    ## L'Ecuyer-CMRG
    if (is.null(seed))
        seed <- .rng_init_stream(seed)
    nextRNGStream(seed)
}

.rng_next_substream <-
    function(seed)
{
    if (is.null(seed))
        seed <- .rng_init_stream(seed)
    nextRNGSubStream(seed)
}

## a random number stream independent of the stream used by R. Use for
## port and other 'internal' assignments without changing the random
## number sequence of users.
.rng_internal_stream <- local({
    state <- .rng_get_generator()
    RNGkind("L'Ecuyer-CMRG") # sets .Random.seed to non-NULL value
    internal_seed <- .Random.seed
    .rng_reset_generator(state$kind, state$seed)

    list(set = function() {
        state <<- .rng_get_generator()
        internal_seed <<- .rng_reset_generator("L'Ecuyer-CMRG", internal_seed)
    }, reset = function() {
        internal_seed <<- .rng_get_generator()$seed
        .rng_reset_generator(state$kind, state$seed)
    })
})

## .rng_seeds_by_task(): generate seeds for random number streams for
## each task, taking into account the number of jobs in a task. Save
## only the stream seed for each task, rather than for each job, and
## use .rng_job_fun_factory() to iterate through the job seeds. In
## this way, the 'geometry' of task / job partitioning does not
## influence the streams used by each job -- each of jobs J1-J5 in T1:
## J1, J2; T2: J3, J4, J5 have the same streams as T1: J1, J2; T2: J3,
## J4; T3: J5.
##
## compute_element is a vector equal in length to the original number
## of elements. TRUE values of compute_element represent elements that
## are included in the task_lengths.
.rng_seeds_by_task <-
    function(BPPARAM, compute_element, task_lengths)
{
    state <- .rng_get_generator()
    on.exit(.rng_reset_generator(state$kind, state$seed))

    n <- length(compute_element)     # how many values to generate?
    n_keep <- sum(task_lengths > 0L) # how many values to keep?
    keep0 <- which(compute_element)
    keep <- keep0[head(cumsum(c(1L, task_lengths[task_lengths > 0L])), -1L)]

    stream_seed <- .bpnextRNGstream(BPPARAM)
    SEED <- .rng_reset_generator("L'Ecuyer-CMRG", stream_seed)$seed

    task_seeds <- rep(list(integer()), length(task_lengths))
    ith_task <- 1L
    for (i in seq_len(n)) {
        ## generate a seed for each element of X
        SEED <- .rng_next_substream(SEED)
        if (i %in% keep) {
            ## keep only the first element of each task
            task_seeds[[ith_task]] <- SEED
            ith_task <- ith_task + 1L
        }
    }

    ## task_seeds were generated without 'gaps', e.g., when
    ## task_lengths = c(2, 0, 1), the first two elements of task_seeds
    ## have been filled in, but we want the first and third. The next
    ## two lines rearrange the generated task_seeds, and set the
    ## 0-length seeds to integer(0)
    task_seeds[task_lengths > 0L] <- task_seeds[lengths(task_seeds) > 0L]
    task_seeds[task_lengths == 0L] <- list(integer())

    task_seeds
}

## .rng_job_fun_factory(): use in conjunction with
## .rng_seeds_by_task() and .rng_lapply() to coordinate streams across
## jobs, independent of how jobs are organized into tasks.
.rng_job_fun_factory <-
    function(FUN, SEED)
{
    ## 'on.exit' is of .rng_job_fun_factory(), not when the anonymous
    ## function goes out of scope
    state <- .rng_get_generator()
    on.exit(.rng_reset_generator(state$kind, state$seed))

    force(FUN)
    SEED <- .rng_reset_generator("L'Ecuyer-CMRG", SEED)$seed

    function(...) {
        ## get / reset the stream each time the function is evaluated
        state <- .rng_get_generator()
        on.exit(.rng_reset_generator(state$kind, state$seed))

        .rng_reset_generator("L'Ecuyer-CMRG", SEED)
        result <- FUN(...)
        SEED <<- .rng_next_substream(SEED)
        result
    }
}

## lapply, but with 'FUN()' wrapped so that each call uses a new
## random number stream
.rng_lapply <-
    function(X, FUN, ..., BPRNGSEED)
{
    state <- .rng_get_generator()
    on.exit(.rng_reset_generator(state$kind, state$seed))

    FUN <- .rng_job_fun_factory(FUN, BPRNGSEED)
    lapply(X, FUN, ...)
}
