### =========================================================================
### Error handling
### -------------------------------------------------------------------------

bpok <-
    function(x, type = bperrorTypes())
{
    x <- bpresult(x)
    type <- match.arg(type)
    !vapply(x, inherits, logical(1), type)
}

bptry <- function(expr, ..., bplist_error, bperror)
{
    if (missing(bplist_error))
        bplist_error <- bpresult

    if (missing(bperror))
        bperror <- identity

    tryCatch(expr, ..., bplist_error=bplist_error, bperror=bperror)
}

bpresult <- function(x)
{
    if (is(x, "bplist_error"))
        x <- attr(x, "result")
    x
}

.composeTry <- function(FUN, log, stop.on.error,
                        stop.immediate = FALSE, # TRUE for SerialParam lapply
                        as.error = TRUE,        # FALSE for BatchJobs compatible
                        timeout,
                        exportglobals = TRUE,
                        force.GC = FALSE)
{
    if (!stop.on.error && stop.immediate)
        stop("[internal] 'stop.on.error == FALSE' && 'stop.immediate == TRUE'")

    FUN <- match.fun(FUN)
    force(log)
    force(stop.on.error)
    force(stop.immediate)
    force(as.error)
    force(timeout)
    force(force.GC)
    if (exportglobals) {
        blocklist <- c(
            "askpass", "asksecret", "buildtools.check",
            "buildtools.with", "pager", "plumber.swagger.url",
            "profvis.print", "restart", "reticulate.repl.hook",
            "reticulate.repl.initialize", "reticulate.repl.teardown",
            "shiny.launch.browser", "terminal.manager", "error",
            "topLevelEnvironment"
        )
        global_options <- base::options()
        global_options <- global_options[!names(global_options) %in% blocklist]
    }

    ERROR_OCCURRED <- FALSE
    UNEVALUATED <- .error_unevaluated() # singleton

    handle_warning <- function(w) {
        .log_warn(log, "%s", w)
        w       # FIXME: muffleWarning; don't rely on capture.output()
    }

    handle_error <- function(e) {
        ERROR_OCCURRED <<- TRUE
        .log_error(log, "%s", e)
        call <- sapply(sys.calls(), deparse, nlines=3)
        e <- if (as.error) {
            .error_remote(e, call)
        } else {
            .condition_remote(e, call) # BatchJobs
        }
        if (stop.immediate)
            stop(e)
        else e
    }

    function(...) {
        setTimeLimit(timeout, timeout, TRUE)
        on.exit(setTimeLimit(Inf, Inf, FALSE))

        if (exportglobals)
            base::options(global_options)

        if (stop.on.error && ERROR_OCCURRED) {
            UNEVALUATED
        } else {
            output <- withCallingHandlers({
                tryCatch({
                    FUN(...)
                }, error=handle_error)
            }, warning=handle_warning)

            ## Trigger garbage collection to cut down on memory usage within
            ## each worker in shared memory contexts. Otherwise, each worker is
            ## liable to think the entire heap is available (leading to each
            ## worker trying to fill said heap, causing R to exhaust memory).
            if (force.GC)
                gc(verbose=FALSE, full=FALSE)

            output
        }
    }
}

.condition_remote <- function(x, call) {
    ## BatchJobs does not return errors
    structure(x, class = c("remote_error", "bperror", "condition"),
              traceback = capture.output(traceback(call)))
}

.error <- function(msg, class=NULL) {
    structure(list(message=msg),
              class = c(class, "bperror", "error", "condition"))
}

.error_remote <- function(x, call) {
    structure(x, class = c("remote_error", "bperror", "error", "condition"),
              traceback = capture.output(traceback(call)))
}

.error_unevaluated <- function()
{
    structure(list(message="not evaluated due to previous error"),
              class=c("unevaluated_error", "bperror", "error", "condition"))
}

.error_not_available <- function(msg)
{
    structure(list(message=msg),
              class=c("not_available_error", "bperror", "error", "condition"))
}

.error_worker_comm <- function(error, msg) {
    msg <- sprintf("%s:\n  %s", msg, conditionMessage(error))
    structure(list(message=msg, original_error_class=class(error)),
              class=c("worker_comm_error", "bperror", "error", "condition"))
}

bperrorTypes <-
    function()
{
    subclasses <- paste0(
        c("remote", "unevaluated", "not_available", "worker_comm"),
        "_error"
    )
    c("bperror", subclasses)
}

.error_bplist <- function(result) {
    remote_error <-
        !bpok(result, "remote_error") |
        !bpok(result, "worker_comm_error")
    idx <- which(remote_error)
    n_remote_error <- sum(remote_error)
    n_other_error <- sum(!bpok(result)) - n_remote_error

    fmt = paste(
        "BiocParallel errors",
        "%d remote errors, element index: %s%s",
        "%d unevaluated and other errors",
        "first remote error:\n%s",
        sep = "\n  "
    )
    first_error <- result[[idx[[1]]]]
    class(first_error) <- tail(class(first_error), 2L)
    first_error_message <- as.character(first_error)
    message <- sprintf(
        fmt,
        n_remote_error,
        paste(head(idx), collapse = ", "),
        ifelse(length(idx) > 6, ", ...", ""),
        n_other_error,
        as.character(result[[idx[[1]]]])
    )

    err <- structure(
        list(message=message),
        result=result,
        class = c("bplist_error", "bperror", "error", "condition"))
}

print.remote_error <- function(x, ...) {
    NextMethod(x)
    cat("traceback() available as 'attr(x, \"traceback\")'\n")
}

`print.bplist_error` <- function(x, ...) {
    NextMethod(x)
    cat("results and errors available as 'attr(x, \"result\")'\n")
}
