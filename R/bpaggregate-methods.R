### =========================================================================
### bpaggregate methods
### -------------------------------------------------------------------------

## All params use bpaggregate,data.frame,BiocParallelParam.
## bpaggretate() dispatches to bplapply() where errors and
## logging are handled.


setMethod("bpaggregate", c("ANY", "missing"),
    function(x, ..., BPREDO=list(), BPPARAM=bpparam(), BPOPTIONS = bpoptions())
{
    bpaggregate(x, ..., BPREDO=BPREDO, BPPARAM=BPPARAM, BPOPTIONS = BPOPTIONS)
})

setMethod("bpaggregate", c("matrix", "BiocParallelParam"),
    function(x, by, FUN, ..., simplify=TRUE,
             BPREDO=list(), BPPARAM=bpparam(), BPOPTIONS = bpoptions())
{
    if (!is.data.frame(x))
        x <- as.data.frame(x)
    bpaggregate(x, by, FUN, ..., simplify=simplify,
                BPREDO=BPREDO, BPPARAM=BPPARAM, BPOPTIONS = BPOPTIONS)
})

setMethod("bpaggregate", c("data.frame", "BiocParallelParam"),
    function(x, by, FUN, ..., simplify=TRUE,
             BPREDO=list(),  BPPARAM=bpparam(), BPOPTIONS = bpoptions())
{
    FUN <- match.fun(FUN)
    if (!is.data.frame(x))
        x <- as.data.frame(x)
    if (!is.list(by))
        stop("'by' must be a list")
    by <- lapply(by, as.factor)

    wrapper <- function(.ind, .x, .AGGRFUN, ..., .simplify)
    {
        sapply(.x[.ind,, drop=FALSE], .AGGRFUN, ..., simplify=.simplify)
    }
    ind <- Filter(length, split(seq_len(nrow(x)), by))
    grp <- rep(seq_along(ind), lengths(ind))
    grp <- grp[match(seq_len(nrow(x)), unlist(ind))]
    res <- bplapply(ind, wrapper, .x=x, .AGGRFUN=FUN, .simplify=simplify,
                    BPREDO=BPREDO, BPPARAM=BPPARAM, BPOPTIONS = BPOPTIONS)
    res <- do.call(rbind, lapply(res, rbind))

    if (is.null(names(by)) && length(by)) {
        names(by) <- sprintf("Group.%i", seq_along(by))
    } else {
        ind <- which(!nzchar(names(by)))
        names(by)[ind] <- sprintf("Group.", ind)
    }

    tab <- as.data.frame(lapply(by, as.character), stringsAsFactors=FALSE)
    tab <- tab[match(sort(unique(grp)), grp),, drop=FALSE]
    rownames(tab) <- rownames(res) <- NULL
    tab <- cbind(tab, res)

    names(tab) <- c(names(by), names(x))
    tab
})

setMethod("bpaggregate", c("formula", "BiocParallelParam"),
    function (x, data, FUN, ..., BPREDO=list(),
              BPPARAM=bpparam(), BPOPTIONS = bpoptions())
{
    if (length(x) != 3L)
        stop("Formula 'x' must have both left and right hand sides")

    m <- match.call(expand.dots=FALSE)
    if (is.matrix(eval(m$data, parent.frame())))
        m$data <- as.data.frame(data)
    m$... <- m$FUN <- m$BPPARAM <- m$BPREDO <- m$BPOPTIONS <- NULL
    m[[1L]] <- quote(stats::model.frame)
    names(m)[[2]] <- "formula"

    if (x[[2L]] == ".") {
        rhs <- as.list(attr(terms(x[-2L]), "variables")[-1])
        lhs <- as.call(c(quote(cbind),
            setdiff(lapply(names(data), as.name), rhs)))
        x[[2L]] <- lhs
        m[[2L]] <- x
    }
    mf <- eval(m, parent.frame())

    if (is.matrix(mf[[1L]])) {
        lhs <- as.data.frame(mf[[1L]])
        bpaggregate(lhs, mf[-1L], FUN=FUN, ...,
                    BPREDO=BPREDO, BPPARAM=BPPARAM, BPOPTIONS = BPOPTIONS)
    }
    else bpaggregate(mf[1L], mf[-1L], FUN=FUN, ...,
                     BPREDO=BPREDO, BPPARAM=BPPARAM, BPOPTIONS = BPOPTIONS)
})
