setGeneric("bpaggregate",
    function(x, ..., BPPARAM)
{
    standardGeneric("bpaggregate")
})

setMethod("bpaggregate", c("ANY", "missing"),
    function(x, ..., BPPARAM)
{
    bp <- registered()[[1L]]
    bpaggregate(x, ..., BPPARAM=bp)
})

setMethod("bpaggregate", c("data.frame", "BiocParallelParam"),
    function(x, by, FUN, ..., simplify=TRUE, BPPARAM)
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
    grp <- rep(seq_along(ind), sapply(ind, length))
    grp <- grp[match(seq_len(nrow(x)), unlist(ind))]
    res <- bplapply(ind, wrapper, .x=x, .AGGRFUN=FUN, .simplify=simplify,
        BPPARAM=BPPARAM)
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
    function (x, data, FUN, ..., BPPARAM)
{
    if (length(x) != 3L) 
        stop("Formula 'x' must have both left and right hand sides")
    m <- match.call(expand.dots=FALSE)
    if (is.matrix(eval(m$data, parent.frame()))) 
        m$data <- as.data.frame(data)
    m$... <- m$FUN <- m$BPPARAM <- NULL
    m[[1L]] <- as.name("model.frame")
    if (x[[2L]] == ".") {
        rhs <- as.list(attr(terms(x[-2L]), "variables")[-1])
        lhs <- as.call(c(quote(cbind), setdiff(lapply(names(data), as.name),
            rhs)))
        x[[2L]] <- lhs
        m[[2L]] <- x
    }
    mf <- eval(m, parent.frame())
    if (is.matrix(mf[[1L]])) {
        lhs <- as.data.frame(mf[[1L]])
        aggregate(lhs, mf[-1L], FUN=FUN, ..., BPPARAM=BPPARAM)
    }
    else aggregate(mf[1L], mf[-1L], FUN=FUN, ..., BPPARAM=BPPARAM)
})
