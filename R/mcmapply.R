### Derived from parallel version 2.14.1 by R Core Team
mcmapply <- function(FUN, ..., MoreArgs = NULL, SIMPLIFY = TRUE, USE.NAMES = TRUE) {
    FUN <- match.fun(FUN)
    dots <- list(...)
    length.out <- max(sapply(dots, length))
    for (dot in dots) {
        if (length.out %% length(dot) != 0) {
            warning("longer argument not a multiple of length of shorter")
            break
        }
    }
    answer <- mclapply(1:length.out, function(i) {
        subdots <- lapply(dots, function(dot) dot[[(i-1) %% length(dot) + 1]])
        do.call(FUN, c(subdots, MoreArgs))
    })
    if (USE.NAMES && length(dots)) {
        if (is.null(names1 <- names(dots[[1L]])) && is.character(dots[[1L]]))
            names(answer) <- dots[[1L]]
        else if (!is.null(names1))
            names(answer) <- names1
    }
    if (!identical(SIMPLIFY, FALSE) && length(answer))
        simplify2array(answer, higher = (SIMPLIFY == "array"))
    else answer
}
