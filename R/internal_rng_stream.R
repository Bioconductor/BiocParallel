## used for port and other 'internal' assignments
##
## RNGkind() advances the random number stream (so the value of
## .Random.seed needs to be determined before callin), and sets
## .Random.seed (so it is not necessary to set.seed() to ensure that
## the seed is set.

.internal_context_set <-
    function()
{
        oseed <-
            if (exists(".Random.seed", envir = .GlobalEnv, inherits = FALSE)) {
                get(".Random.seed", envir = .GlobalEnv, inherits = FALSE)
            } else NULL
        okind <- RNGkind("L'Ecuyer-CMRG")

        list(kind = okind, seed = oseed)
}

.internal_context_unset <-
    function(value)
{
    RNGkind(value$kind[[1]])
    if (!is.null(value$seed)) {
        assign(".Random.seed", value$seed, envir = .GlobalEnv)
    } else rm(.Random.seed, envir = .GlobalEnv)
}

.internal_rng_stream <- local({
    octx <- .internal_context_set()
    internal_seed <- nextRNGStream(.Random.seed)
    .internal_context_unset(octx)

    list(set = function() {
        value <- .internal_context_set()
        assign(".Random.seed", internal_seed, envir = .GlobalEnv)
        value
    }, unset = function(octx) {
        internal_seed <<-
            get(".Random.seed", envir = .GlobalEnv, inherits = FALSE)
        .internal_context_unset(octx)
    })
})
