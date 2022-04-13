.Reducer <- setRefClass(
    "Reducer",
    fields = list(
        result = "ANY",
        total = "numeric",
        reduced.num = "numeric",
        exists.error = "logical"
    )
)

.LapplyReducer <- setRefClass(
    "LapplyReducer",
    fields = list(),
    contains = "Reducer"
)

.IterateReducer <- setRefClass(
    "IterateReducer",
    fields = list(
        last.reduced.index = "numeric",
        value.cache = "environment",
        errors = "environment",
        REDUCE = "ANY",
        reduce.in.order = "logical",
        init.missing = "logical",
        REDUCE.missing = "logical"
        ),
    contains = "Reducer"
)


setGeneric(".map_index", function(reducer, idx){
    standardGeneric(".map_index")
})

setGeneric(".reducer_add", function(reducer, idx, values){
    standardGeneric(".reducer_add")
})

setGeneric(".reducer_reduce", function(reducer, idx){
    standardGeneric(".reducer_reduce")
})

setGeneric(".reducer_ok", function(reducer){
    standardGeneric(".reducer_ok")
})

setGeneric(".reducer_complete", function(reducer){
    standardGeneric(".reducer_complete")
})

setGeneric(".reducer_value", function(reducer){
    standardGeneric(".reducer_value")
})

#########################
## Reducer
#########################
setMethod(".reducer_complete", signature = "Reducer",
          function(reducer)
{
    reducer$total == reducer$reduced.num
})

setMethod(".reducer_ok", signature = "Reducer",
    function(reducer)
{
    !reducer$exists.error
})
#########################
## LapplyReducer
#########################
.lapplyReducer <-
    function(ntotal, reducer = NULL)
{
    if (is.null(reducer)) {
        result <- rep(list(.error_unevaluated()), ntotal)
    } else {
        result <- reducer$result
        ntotal <- sum(!bpok(result))
    }

    .LapplyReducer(
        result = result,
        total = ntotal,
        reduced.num = 0L,
        exists.error = FALSE
    )
}

setMethod(".reducer_add", signature = "LapplyReducer",
    function(reducer, idx, values)
{
    reducer$result[idx] <- values
    reducer$reduced.num <- reducer$reduced.num + length(idx)

    if(!all(bpok(values)))
        reducer$exists.error <- TRUE
    reducer
})

setMethod(".reducer_value", signature = "LapplyReducer",
    function(reducer)
{
    reducer$result
})

#########################
## IterateReducer
#########################
.iterate_error_index <-
    function(reducer)
{
    if (is.null(reducer))
        return(integer())
    finished_idx <- as.numeric(names(reducer$value.cache))
    missing_idx <- setdiff(seq_len(reducer$total), finished_idx)
    sort(c(missing_idx, as.numeric(names(reducer$errors))))
}

.iterateReducer <-
    function(REDUCE, init, reduce.in.order=FALSE, reducer = NULL)
{
    if (is.null(reducer)) {
        if (missing(init)){
            result <- NULL
            init.missing <- TRUE
        } else {
            result <- init
            init.missing <- FALSE
        }
        if (missing(REDUCE)) {
            REDUCE <- NULL
            REDUCE.missing <- TRUE
        } else {
            REDUCE.missing <- FALSE
        }
        .IterateReducer(
            result = result,
            total = 0L,
            reduced.num = 0L,
            exists.error = FALSE,
            last.reduced.index = 0L,
            value.cache = new.env(parent = emptyenv()),
            errors = new.env(parent = emptyenv()),
            REDUCE = REDUCE,
            reduce.in.order = reduce.in.order,
            init.missing = init.missing,
            REDUCE.missing = REDUCE.missing
        )
    } else {
        reducer <- reducer$copy()
        reducer$value.cache <- as.environment(
            as.list(reducer$value.cache, all.names=TRUE)
            )
        reducer$errors <- as.environment(
            as.list(reducer$errors, all.names=TRUE)
        )
        reducer
    }
}

setMethod(".reducer_add", signature = "IterateReducer",
    function(reducer, idx, values)
{
    reduce.in.order <- reducer$reduce.in.order
    idx <- as.character(idx)
    value <- values[[1]]
    if (.bpeltok(value)) {
        if (exists(idx, envir = reducer$errors)){
            rm(list = idx, envir = reducer$errors)
            reducer$exists.error <- (length(reducer$errors) > 0L)
        }
    } else {
        reducer$errors[[idx]] <- value
        reducer$exists.error <- TRUE
    }
    reducer$value.cache[[idx]] <- value

    reducer$total <- max(reducer$total, as.numeric(idx))

    if (reduce.in.order)
        while (.reducer_reduce(reducer, reducer$last.reduced.index + 1L)) {}
    else
        .reducer_reduce(reducer, idx)

    reducer
})

setMethod(".reducer_reduce", signature = "IterateReducer",
    function(reducer, idx)
{
    idx <- as.character(idx)
    if (!exists(idx, envir = reducer$value.cache)) {
        return(FALSE)
    }

    ## Do not reduce when there is an error and reduce.in.order == TRUE
    if (!.reducer_ok(reducer) && reducer$reduce.in.order)
        return(FALSE)
    ## The cached value is a list of length 1
    value <- reducer$value.cache[[idx]]
    ## Do not reduce the erroneous result
    if (!.bpeltok(value))
        return(FALSE)
    if (!reducer$REDUCE.missing) {
        if (reducer$init.missing && (reducer$reduced.num == 0)) {
            reducer$result <- value
        } else {
            reducer$result <- reducer$REDUCE(reducer$result, value)
        }
        ## DO NOT REMOVE the cache, only set it to NULL
        ## this is used to keep track of the finished results
        reducer$value.cache[[idx]] <- NULL
    }
    reducer$reduced.num <- reducer$reduced.num + 1L
    reducer$last.reduced.index <- as.numeric(idx)
    TRUE
})

setMethod(".reducer_value", signature = "IterateReducer",
    function(reducer)
{
    value.cache <- reducer$value.cache
    if (!reducer$REDUCE.missing) {
        res <- reducer$result
    } else {
        idx <- setdiff(names(value.cache), names(reducer$errors))
        res <- rep(list(NULL), reducer$total)
        for (i in idx)
            res[[as.integer(i)]] <- value.cache[[i]]
    }
    ## Attach the errors as an attribute
    if (!.reducer_ok(reducer) || !.reducer_complete(reducer)) {
        ## cannot attach attribute to NULL
        if (is.null(res)) res <- list()

        error_index <- .iterate_error_index(reducer)
        all_errors <- rep(list(.error_unevaluated()), length(error_index))
        names(all_errors) <- as.character(error_index)

        non_missing_errors <- as.list(reducer$errors)
        all_errors[names(non_missing_errors)] <- non_missing_errors
        attr(res, ".bperrors") <- all_errors
    }
    res
})
