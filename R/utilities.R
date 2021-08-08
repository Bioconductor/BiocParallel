.getEnvironmentVariable <-
    function(variable, default = NA_integer_)
{
    result <- withCallingHandlers({
        value <- Sys.getenv(variable, default)
        as.integer(value)
    }, warning = function(w) {
        txt <- sprintf(
            "Trying to coercing the environment variable '%s' to an integer caused a warning. The value of the environment variable was '%s'. The warning was: %s",
            variable, value, conditionMessage(w)
        )
        .warning(txt)
        invokeRestart("muffleWarning")
    })

    if (!is.na(result) && (result <= 0L)) {
        txt <- sprintf(
            "The environment variable '%s' must be > 0. The value was '%d'.",
            variable, result
        )
        .stop(txt)
    }

    result
}

.detectCores <- function() {
    ## environment variables; least to most compelling
    result <- .getEnvironmentVariable("R_PARALLELLY_AVAILABLECORES_FALLBACK")
    result <- .getEnvironmentVariable("BIOCPARALLEL_WORKER_NUMBER", result)

    ## fall back to detectCores() if necessary
    if (is.na(result)) {
        result <- parallel::detectCores()
        if (is.na(result))
            result <- 1L
        result <- max(1L, result - 2L)
    }

    ## respect 'mc.cores', overriding env. variables an detectCores()
    result <- getOption("mc.cores", result)

    ## override user settings by build-system configurations
    if (nzchar(Sys.getenv("_R_CHECK_LIMIT_CORES_")))
        result <- min(result, 2L)
    if (nzchar(Sys.getenv("BBS_HOME")))
        result <- min(result, 4L)

    result
}

.splitIndices <- function (nx, tasks)
{
    ## derived from parallel
    i <- seq_len(nx)
    if (nx == 0L)
        list()
    else if (tasks <= 1L || nx == 1L)  # allow nx, nc == 0
        list(i)
    else {
        fuzz <- min((nx - 1L)/1000, 0.4 * nx / tasks)
        breaks <- seq(1 - fuzz, nx + fuzz, length.out = tasks + 1L)
        si <- structure(split(i, cut(i, breaks)), names = NULL)
        si[sapply(si, length) != 0]
    }
}

.splitX <- function(X, workers, tasks) 
{
    if (tasks == 0L) {
        tasks <- workers
    } else {
        tasks <- min(length(X), tasks)
    }
    idx <- .splitIndices(length(X), tasks)
    relist(X, idx)
}

.redo_index <-
    function(X, BPREDO)
{
    idx <- !bpok(BPREDO)
    if (length(idx)) {
        if (length(BPREDO) != length(X))
            stop("'length(BPREDO)' must equal 'length(X)'")
        if (!any(idx))
            stop("no previous error in 'BPREDO'")
    }
    idx
}

## re-apply names on X of lapply(X, FUN) to the return value
.rename <-
    function(results, X)
{
    names(results) <- names(X)
    results
}

## re-apply the names on, e.g., mapply(FUN, X) to the return value;
## see test_mrename for many edge cases
.mrename <-
    function(results, dots, USE.NAMES=TRUE)
{
    ## dots: a list() containing one element for each ... argument
    ## passed to mapply
    if (USE.NAMES) {
        ## extract the first argument; if there are no arguments, then
        ## dots is (unnamed) list(0)
        if (length(dots))
            dots <- dots[[1L]]
        if (is.character(dots) && is.null(names(dots))) {
            names(results) <- dots
        } else {
            names(results) <- names(dots)
        }
    } else {
        results <- unname(results)
    }
    results
}

.simplify <-
    function(results, SIMPLIFY=FALSE)
{
    if (SIMPLIFY && length(results))
        results <- simplify2array(results)
    results
}

.prettyPath <- function(tag, filepath)
{
    wd <- options('width')[[1]] - nchar(tag) - 6
    if (length(filepath) == 0 || is.na(filepath))
        return(sprintf("%s: %s", tag, NA_character_))
    if (0L == length(filepath) || nchar(filepath) < wd)
        return(sprintf("%s: %s", tag, filepath))
    bname <- basename(filepath)
    wd1 <- wd - nchar(bname)
    dname <- substr(dirname(filepath), 1, wd1)
    sprintf("%s: %s...%s%s",
            tag, dname, .Platform$file.sep, bname)
}

.getDotsForMapply <-
    function(...)
{
    ddd <- list(...)
    if (!length(ddd))
        return(list(list()))
    len <- vapply(ddd, length, integer(1L))
    if (!all(len == len[1L])) {
        max.len <- max(len)
        if (max.len && any(len == 0L))
            stop("zero-length and non-zero length inputs cannot be mixed")
        if (any(max.len %% len))
            warning("longer argument not a multiple of length of vector")
        ddd <- lapply(ddd, rep_len, length.out=max.len)
    }

    ddd
}

.dir_valid_rw <-
    function(x)
{
    all(file.access(x, 6L) == 0L)
}

.warning <- function(...) {
    msg <- paste(
        strwrap(paste0("\n", ...), indent = 2, exdent = 2), collapse="\n"
    )
    warning(msg, call. = FALSE)
}

.stop <- function(...) {
    msg <- paste(
        strwrap(paste0("\n", ...), indent = 2, exdent = 2), collapse="\n"
    )
    stop(msg, call. = FALSE)
}
