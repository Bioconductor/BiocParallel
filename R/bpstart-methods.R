### =========================================================================
### ClusterManager object: ensures started clusters are stopped
### -------------------------------------------------------------------------

.ClusterManager <- local({
    ## package-global registry of backends; use to avoid closing
    ## socket connections of unreferenced backends during garbage
    ## collection -- bpstart(MulticoreParam(1)); gc(); gc()
    uid <- 0
    env <- environment()
    list(add = function(cluster) {
        uid <<- uid + 1L
        cuid <- as.character(uid)
        env[[cuid]] <- cluster          # protection
        cuid
    }, drop = function(cuid) {
        if (length(cuid) && cuid %in% names(env))
            rm(list=cuid, envir=env)
        invisible(NULL)
    }, get = function(cuid) {
        env[[cuid]]
    }, ls = function() {
        cuid <- setdiff(ls(env), c("uid", "env"))
        cuid[order(as.integer(cuid))]
    })
})

### =========================================================================
### bpstart() methods
### -------------------------------------------------------------------------

setMethod("bpstart", "ANY", function(x, ...) invisible(x))

setMethod("bpstart", "missing",
    function(x, ...)
{
    x <- registered()[[1]]
    bpstart(x)
})

##
## .bpstart_impl: common functionality after bpisup()
##

.bpstart_error_handler <-
    function(x, response, id)
{
    value <- lapply(response, function(elt) elt[["value"]][["value"]])
    if (!all(bpok(value))) {
        on.exit(try(bpstop(x)))
        stop(
            "\nbpstart() ", id, " error:\n",
            conditionMessage(.error_bplist(value))
        )
    }
}

.bpstart_set_rng_stream <-
    function(x)
{
    ## initialize the random number stream; increment the stream only
    ## in bpstart_impl
    .RNGstream(x) <- .rng_init_stream(bpRNGseed(x))

    invisible(.RNGstream(x))
}


.bpstart_set_finalizer <-
    function(x)
{
    if (length(x$.uid) == 0L) {
        finalizer_env <- as.environment(list(self=x$.self))
        reg.finalizer(
            finalizer_env, function(e) bpstop(e[["self"]]), onexit=TRUE
        )
        x$.finalizer_env <- finalizer_env
    }
    x$.uid <- .ClusterManager$add(bpbackend(x))

    invisible(x)
}

.bpstart_impl <-
    function(x)
{
    ## common actions once bpisup(backend)

    ## initialize the random number stream
    .bpstart_set_rng_stream(x)

    ## clean up when x left open
    .bpstart_set_finalizer(x)
}
