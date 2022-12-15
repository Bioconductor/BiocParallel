### =========================================================================
### bpmapply methods
### -------------------------------------------------------------------------

# see test_utilities.R:test_transposeArgsWithIterations() for all
# USE.NAMES corner cases
.transposeArgsWithIterations <- function(nestedList, USE.NAMES) {
    num_arguments <- length(nestedList)
    if (num_arguments == 0L) {
        return(list())
    }

    ## nestedList[[1L]] has the values for the first argument in all
    ## iterations
    num_iterations <- length(nestedList[[1L]])

    ## count the iterations, and name them if needed
    iterations <- seq_len(num_iterations)
    if (USE.NAMES) {
        first_arg <- nestedList[[1L]]
        if (is.character(first_arg) && is.null(names(first_arg))) {
            names(iterations) <- first_arg
        } else {
            names(iterations) <- names(first_arg)
        }
    }

    ## argnames:
    argnames <- names(nestedList)

    ## on iteration `i` we get the i-th element from each list. Note
    ## that .getDotsForMapply() has taken care already of ensuring
    ## that nestedList elements are recycled properly
    lapply(iterations, function(i) {
        x <- lapply(nestedList, function(argi) {
            unname(argi[i])
        })
        names(x) <- argnames
        x
    })
}

## bpmapply() dispatches to bplapply() where errors and logging are handled.

setMethod("bpmapply", c("ANY", "BiocParallelParam"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE,
             USE.NAMES=TRUE, BPREDO=list(),
             BPPARAM=bpparam(), BPOPTIONS = bpoptions())
{
    ## re-package for lapply
    ddd <- .getDotsForMapply(...)
    FUN <- match.fun(FUN)

    if (!length(ddd))
      return(list())
     
    ddd <- .transposeArgsWithIterations(ddd, USE.NAMES)
    if (!length(ddd))
      return(ddd)

    .wrapMapplyNotShared <- local({
        function(dots, .FUN, .MoreArgs) {
            .mapply(.FUN, dots, .MoreArgs)[[1L]]
        }
    }, envir = baseenv())

    res <- bplapply(
        X=ddd, .wrapMapplyNotShared, .FUN=FUN,
        .MoreArgs=MoreArgs, BPREDO=BPREDO,
        BPPARAM=BPPARAM, BPOPTIONS = BPOPTIONS
    )
    .simplify(res, SIMPLIFY)
})

setMethod("bpmapply", c("ANY", "missing"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE,
             USE.NAMES=TRUE, BPREDO=list(),
             BPPARAM=bpparam(), BPOPTIONS = bpoptions())
{
    FUN <- match.fun(FUN)
    bpmapply(FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
             USE.NAMES=USE.NAMES, BPREDO=BPREDO,
             BPPARAM=BPPARAM, BPOPTIONS = BPOPTIONS)
})

setMethod("bpmapply", c("ANY", "list"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE,
             USE.NAMES=TRUE, BPREDO=list(),
             BPPARAM=bpparam(), BPOPTIONS = bpoptions())
{
    FUN <- match.fun(FUN)

    if (!all(vapply(BPPARAM, inherits, logical(1), "BiocParallelParam")))
        stop("All elements in 'BPPARAM' must be BiocParallelParam objects")
    if (length(BPPARAM) == 0L)
        stop("'length(BPPARAM)' must be > 0")

    myFUN <-
        if (length(BPPARAM) > 1L) {
          if (length(param <- BPPARAM[-1]) == 1L)
            function(...) FUN(..., BPPARAM=param[[1]])
          else
            function(...) FUN(..., BPPARAM=param)
        } else FUN
    bpmapply(myFUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
             USE.NAMES=USE.NAMES, BPREDO=BPREDO,
             BPPARAM=BPPARAM[[1L]], BPOPTIONS = BPOPTIONS)
})

