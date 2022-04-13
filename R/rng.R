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

## A seed generator which accept a vector 'index' as input
## return the seed stream corresponding to redo_index[index[1]]
## For example, if redo_index = {4, 8} and index = {2},
## The generator should iterate the initial seed 7 times and
## return the 8th seed stream
.seed_generator <- function(init_seed, redo_index){
    if (redo_index[1] == 1L)
        seed <- init_seed
    else
        seed <- .rng_iterate_substream(init_seed, redo_index[1] - 1L)
    seed_index <- 1L
    seed_gaps <- c(0, diff(redo_index))

    seed_original_index <- out_of_range_vector(redo_index)
    ## The seed index of the next task
    ## always larger than any index in the current task
    next_task_seed_index <- tail(redo_index, 1L) + 1L


    ## Cache a few seeds as an anchor to speed up the seed iteration for
    ## random seed index
    seed_space <- rep(list(NULL), min(length(redo_index), 1000))
    cache_range <- ceiling(length(redo_index)/length(seed_space))
    seed_space[[1]] <- seed

    ## input: an increasing index of redo_index ranged from 1 to length(redo_index)
    ##        if the index is NULL, return the last seed iterated by its length
    ## output: the seed corresponding to the first index
    function(index = NULL){
        if (is.null(index))
            return(.rng_iterate_substream(seed, next_task_seed_index - seed_index))

        first_index <- index[[1]]
        end_index <- tail(index, 1L)

        ## Recompute the index of the next task
        ## we only need to do it when the index is out of range
        ## this happens in bpiterate as we do not know the length
        ## of the iterator in advance
        if (end_index > length(redo_index)){
            next_seed_index <- seed_original_index(end_index) + 1L
            next_task_seed_index <<- max(
                next_task_seed_index,
                next_seed_index
            )
        }

        ## Check if the seed_space contains the seed which
        ## we can use as a shortcut to speed up the iteration
        cache_id <- (first_index - 1L)%/%cache_range + 1L
        cache_id <- min(cache_id, length(seed_space))
        cache_seed <- seed_space[[cache_id]]

        if (is.null(cache_seed)) {
            ## If no cache is found, we iterate the seed from the current seed
            iter_start_index <- seed_index
            iter_seed <- seed
        } else {
            ## If there is a cached seed, we start from the cached seed
            iter_start_index <- (cache_id - 1L) * cache_range + 1L
            iter_seed <- cache_seed
            ## use the current seed if the current seed is more efficient
            if (seed_index <= first_index &&
                iter_start_index < seed_index) {
                iter_start_index <- seed_index
                iter_seed <- seed
            }
        }

        ## iterate the seed
        for (i in seq_len(first_index - iter_start_index)) {
            redo_seed_index <- iter_start_index + i
            if (redo_seed_index <= length(seed_gaps))
                seed_gap <- seed_gaps[redo_seed_index]
            else
                seed_gap <- 1L
            iter_seed <- .rng_iterate_substream(iter_seed, seed_gap)
            ## Cache the seed value if it hits the anchor point
            if ((redo_seed_index - 1L)%%cache_range == 0)
                seed_space[[(redo_seed_index - 1L)%/%cache_range + 1L]] <<- iter_seed
        }

        ## the variable seed always holds the latest seed
        ## and the largest length
        if (first_index >= seed_index) {
            seed_index <<- first_index
            seed <<- iter_seed
        }
        return(iter_seed)
    }
}