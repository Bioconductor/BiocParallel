setMethod(bpvectorize, c("ANY", "ANY"),
    function(FUN, VECTOR.ARGS, ..., AGGREGATE=c, BPPARAM)
{
    match.fun(FUN)
})

setMethod(bpvectorize, c("ANY", "BiocParallelParam"),
    function(FUN, VECTOR.ARGS, ..., AGGREGATE=c, BPPARAM)
{
    FUN <- match.fun(FUN)

    ## Get a character vector of names of vectorizable args for FUN
    arg.names <- namable.args(FUN)
    if (length(arg.names) == 0) {
        stop("Cannot determine argument names for FUN.")
    }

    ## Default is to vectorize first arg
    if (missing(VECTOR.ARGS))
        VECTOR.ARGS <- arg.names[1]
    ## NA means vectorize all args
    else if (is.na(VECTOR.ARGS))
        VECTOR.ARGS <- arg.names

    ## Check that FUN actually has the requested args
    if (!all(VECTOR.ARGS %in% arg.names))
        stop("Requested args do not exist: ",
             deparse(setdiff(VECTOR.ARGS, arg.names)))

    ## Construct a wrapper that calls FUN through bpmvec for
    ## parallelization
    FUNPV <- function() {
        ## This gets all args into a list.
        args <- lapply(as.list(match.call())[-1L], eval, parent.frame())
        ## Split args into vector and scalar
        dovec <- names(args) %in% VECTOR.ARGS
        vector.args <- args[dovec]
        scalar.args <- args[!dovec]
        ## Construct appropriate arglist for bpmvec
        ## (i.e.: ...=vector.args, MoreArge=scalar.args)
        arglist <- c(list(FUN=FUN,
                          MoreArgs=scalar.args,
                          AGGREGATE=AGGREGATE,
                          BPPARAM=BPPARAM),
                     ## This is the "..." arguments
                     vector.args)
        do.call(bpmvec, arglist)
    }
    formals(FUNPV) <- .formals(FUN)
    FUNPV
})

setMethod(bpvectorize, c("ANY", "missing"),
    function(FUN, VECTOR.ARGS, ..., AGGREGATE=c, BPPARAM)
{
    FUN <- match.fun(FUN)
    x <- registered()[[1]]
    bpvectorize(FUN, VECTOR.ARGS=VECTOR.ARGS, ..., AGGREGATE=AGGREGATE, BPPARAM=x)
})
