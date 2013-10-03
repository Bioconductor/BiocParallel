setMethod(bpvectorize, c("ANY", "ANY"),
    function(FUN, VECTORIZE.ARGS, ..., AGGREGATE=c, BPPARAM)
{
    return match.fun(FUN)
})

setMethod(bpvectorize, c("ANY", "BiocParallelParam"),
    function(FUN, VECTORIZE.ARGS, ..., AGGREGATE=c, BPPARAM)
{
    FUN <- match.fun(FUN)

    ## Get a character vector of names of vectorizable args for FUN
    arg.names <- names(as.list(.formals(FUN)))
    ## Can't vectorize dots
    arg.names <- setdiff(arg.names, "...")
    if (length(arg.names) == 0) {
        stop("Cannot determine argument names for FUN.")
    }

    ## Check that FUN actually has the requested args
    if (!all(VECTORIZE.ARGS %in% arg.names))
        stop("Requested args do not exist: ",
             deparse(setdiff(VECTORIZE.ARGS, arg.names)))

    FUNPV <- function() {
        ## This gets all args into a list.
        args <- lapply(as.list(match.call())[-1L], eval, parent.frame())
        dovec <- names(args) %in% VECTORIZE.ARGS
        vector.args <- args[dovec]
        scalar.args <- args[!dovec]
        arglist <- c(list(FUN=FUN),
                     vector.args,
                     list(MoreArgs=scalar.args,
                          AGGREGATE=AGGREGATE,
                          BPPARAM=BPPARAM))
        do.call(bpmvec, arglist)
    }
    formals(FUNPV) <- .formals(FUN)
    FUNPV
})

setMethod(bpvectorize, c("ANY", "missing"),
    function(FUN, VECTORIZE.ARGS, ..., AGGREGATE=c, BPPARAM)
{
    FUN <- match.fun(FUN)
    x <- registered()[[1]]
    bpvectorize(FUN, VECTORIZE.ARGS=VECTORIZE.ARGS, ..., AGGREGATE=AGGREGATE, BPPARAM=x)
})
