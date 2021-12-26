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
    if (!is.null(seed)) {
        RNGkind("default", "default", "default")
        set.seed(seed)

        ## change kind
        kind <- "L'Ecuyer-CMRG"
        RNGkind(kind)

        ## .Random.seed always exists after RNGkind()
        seed <- get(".Random.seed", envir = .GlobalEnv, inherits = FALSE)
    } else {
        .rng_internal_stream$set()
        seed <- get(".Random.seed", envir = .GlobalEnv, inherits = FALSE)
        ## advance internal stream by 1
        runif(1)
        .rng_internal_stream$reset()
    }

    seed
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

## iterate the seed stream n times
.rng_iterate_substream <-
    function(seed, n)
{
    for (k in seq_len(n))
        seed <- .rng_next_substream(seed)
    seed
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
