.Reducer <- setRefClass(
    "Reducer",
    fields = list(
        result = "ANY",
        total = "numeric",
        reduced.num = "numeric",
        reduced.index = "numeric",
        value.cache = "environment",
        redo.index = "numeric"
    )
)

.LapplyReducer <- setRefClass(
    "LapplyReducer",
    fields = list(
        exists.error = "logical"
    ),
    contains = "Reducer"
)

.IterateReducer <- setRefClass(
    "IterateReducer",
    fields = list(
        REDUCE = "ANY",
        errors = "environment",
        reduce.in.order = "logical",
        appending.offset = "numeric",
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
    length(reducer$errors) == 0L
})
#########################
## LapplyReducer
#########################
.lapplyReducer <-
    function(ntotal, reducer = NULL)
{
    if (is.null(reducer)) {
        result <- rep(list(.error_unevaluated()), ntotal)
        redo.index <- seq_len(ntotal)
    } else {
        result <- reducer$result
        redo.index <- which(!bpok(result))
        ntotal <- length(redo.index)
    }

    .LapplyReducer(
        result = result,
        total = ntotal,
        reduced.index = 1L,
        reduced.num = 0L,
        value.cache = new.env(parent = emptyenv()),
        redo.index = redo.index,
        exists.error = FALSE
    )
}

setMethod(".reducer_add", signature = "LapplyReducer",
    function(reducer, idx, values)
{
    reducer$value.cache[[as.character(idx)]] <- values

    while (.reducer_reduce(reducer, reducer$reduced.index)) {}

    if(!all(bpok(values)))
        reducer$exists.error <- TRUE

    reducer
})

setMethod(".reducer_reduce", signature = "LapplyReducer",
    function(reducer, idx)
{
    ## obtain the cached value
    idx <- as.character(idx)
    if (!exists(idx, envir = reducer$value.cache))
        return(FALSE)
    values <- reducer$value.cache[[idx]]
    rm(list = idx, envir = reducer$value.cache)

    ## Find the true index of the reduced value in the result
    idx <- reducer$redo.index[reducer$reduced.num + 1L]
    reducer$result[idx - 1L + seq_along(values)] <- values

    reducer$reduced.index <- reducer$reduced.index + 1L
    reducer$reduced.num <- reducer$reduced.num + length(values)
    TRUE
})

setMethod(".reducer_value", signature = "LapplyReducer",
    function(reducer)
{
    reducer$result
})

setMethod(".reducer_ok", signature = "LapplyReducer",
    function(reducer)
{
    !reducer$exists.error
})

#########################
## IterateReducer
#########################
.redo_index_iterate <-
    function(reducer)
{
    if (is.null(reducer))
        return(integer())
    finished_idx <- as.integer(names(reducer$value.cache))
    missing_idx <- setdiff(seq_len(reducer$total), finished_idx)
    c(missing_idx, as.integer(names(reducer$errors)))
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
            reduced.index = 1L,
            value.cache = new.env(parent = emptyenv()),
            redo.index = integer(),
            REDUCE = REDUCE,
            errors = new.env(parent = emptyenv()),
            reduce.in.order = reduce.in.order,
            appending.offset = 0L,
            init.missing = init.missing,
            REDUCE.missing = REDUCE.missing
        )
    } else {
        reducer <- reducer$copy()
        reducer$appending.offset <- reducer$total
        reducer$redo.index <- .redo_index_iterate(reducer)
        reducer$value.cache <- as.environment(
            as.list(reducer$value.cache, all.names=TRUE)
            )
        reducer$errors <- as.environment(
            as.list(reducer$errors, all.names=TRUE)
        )
        reducer
    }
}

setMethod(".map_index", signature = "IterateReducer",
    function(reducer, idx)
{
    redo.index <- reducer$redo.index
    if (idx <= length(redo.index))
        idx <- redo.index[idx]
    else
        idx <- idx - length(redo.index) + reducer$appending.offset
    idx
})

setMethod(".reducer_add", signature = "IterateReducer",
    function(reducer, idx, values)
{
    reduce.in.order <- reducer$reduce.in.order
    idx <- as.character(.map_index(reducer, idx))
    value <- values[[1]]
    if (.bpeltok(value)) {
        if (exists(idx, envir = reducer$errors))
            rm(list = idx, envir = reducer$errors)
    } else {
        reducer$errors[[idx]] <- idx
    }
    reducer$value.cache[[idx]] <- value

    reducer$total <- max(reducer$total, as.numeric(idx))

    if (reduce.in.order)
        while (.reducer_reduce(reducer, reducer$reduced.index)) {}
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

    ## stop reducing when reduce.in.order == TRUE
    ## and we have a pending error
    if (!.reducer_ok(reducer) && reducer$reduce.in.order)
        return(FALSE)
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
        ## DO NOT REMOVE, only set to NULL to keep track
        ## of the finished results
        reducer$value.cache[[idx]] <- NULL
    }
    reducer$reduced.num <- reducer$reduced.num + 1L
    reducer$reduced.index <- reducer$reduced.index + 1L
    TRUE
})

setMethod(".reducer_value", signature = "IterateReducer",
    function(reducer)
{
    value.cache <- reducer$value.cache
    if (!reducer$REDUCE.missing) {
        res <- reducer$result
    } else {
        ## remove the index of the meta elements and errors
        idx <- names(value.cache)
        idx <- setdiff(idx, names(reducer$errors))
        res <- rep(list(NULL), reducer$total)
        for (i in idx)
            res[[as.integer(i)]] <- value.cache[[i]]
    }
    ## Attach the errors as an attribute
    if (!.reducer_ok(reducer) || !.reducer_complete(reducer)) {
        ## cannot attach attribute to NULL
        if (is.null(res)) res <- list()
        idx <- .redo_index_iterate(reducer)
        errors <- rep(list(.error_unevaluated()), length(idx))
        names(errors) <- as.character(idx)
        for (i in names(reducer$errors))
            errors[[i]] <- value.cache[[i]]
        attr(res, "errors") <- errors
    }
    res
})


