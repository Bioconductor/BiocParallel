### =========================================================================
### Error handling 
### -------------------------------------------------------------------------

bplasterror <- function() {
    .Deprecated(msg = "bplasterror has been deprecated")
}

bpresume <- function(expr) {
    .Deprecated(msg = "bpresume has been deprecated")
}

bpok <- function(x) {
    !vapply(x, inherits, logical(1), "bperror")
}

bptry <- function(expr, ..., bplist_error, bperror)
{
    if (missing(bplist_error))
        bplist_error <- function(err)
            attr(err, "result")

    if (missing(bperror))
        bperror <- identity
    tryCatch(expr, ..., bplist_error=bplist_error, bperror=bperror)
}

.composeTry <- function(FUN, log, stop.on.error,
                        stop.immediate = FALSE, # TRUE for SerialParam lapply
                        as.error = TRUE,        # FALSE for BatchJobs compatible
                        timeout)
{
    if (!stop.on.error && stop.immediate)
        stop("[internal] 'stop.on.error == FALSE' && 'stop.immediate == TRUE'")

    FUN <- match.fun(FUN)
    force(log)
    force(stop.on.error)
    force(stop.immediate)
    force(as.error)
    force(timeout)

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

        if (stop.on.error && ERROR_OCCURRED) {
            UNEVALUATED
        } else {
            withCallingHandlers({
                tryCatch({
                    FUN(...)
                }, error=handle_error)
            }, warning=handle_warning)
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

.error_bplist <- function(result) {
    idx <- which(!bpok(result) &
                  !vapply(result, inherits, logical(1), "not_available_error"))
    err <- structure(list(
        message=sprintf(
            "BiocParallel errors\n  element index: %s%s\n  first error: %s",
            paste(head(idx), collapse=", "),
            if (length(idx) > 6) ", ..." else "",
            conditionMessage(result[[idx[1]]]))),
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
