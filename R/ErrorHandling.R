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
    vapply(x, function(elt) !is(elt, "bperror"), logical(1))
}

.composeTry <- function(FUN, log, stop.on.error,
                        stop.immediate = FALSE, # TRUE for SerialParam lapply
                        as.error = TRUE,        # FALSE for BatchJobs compatible
                        timeout=Inf)
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
        if (log)
            flog.warn("%s", w)
        w
    }

    handle_error <- function(e) {
        ERROR_OCCURRED <<- TRUE
        if (log)
            flog.error("%s", e)
        call <- sapply(sys.calls(), deparse)
        e <- if (as.error) {
            .error_remote(e, call)
        } else .condition_remote(e, call)
        invokeRestart("abort", e)
    }

    handle_abort <- if (!stop.immediate) force else stop

    .try <- function(expr) {
        if (stop.on.error && ERROR_OCCURRED) {
            UNEVALUATED
        } else {
            withRestarts({
                withCallingHandlers(expr, warning=handle_warning, 
                                    error=handle_error)
            }, abort=handle_abort)
        }
    }

    function(...) {
        setTimeLimit(timeout, timeout, TRUE)
        on.exit(setTimeLimit(Inf, Inf, FALSE))
        .try(FUN(...))
    }
}

.condition_remote <- function(x, call) {
    ## BatchJobs does not return errors
    structure(x, class = c("remote-error", "bperror", "condition"),
              traceback = capture.output(traceback(call))) 
}

.error_remote <- function(x, call) {
    structure(x, class = c("remote-error", "bperror", "error", "condition"),
              traceback = capture.output(traceback(call))) 
}

.error_unevaluated <- function()
{
    structure(list(message="not evaluated due to previous error"),
              class=c("unevaluated-error", "bperror", "error", "condition"))
}

.error_worker_comm <- function(error, msg) {
    msg <- sprintf("%s:\n  %s", msg, conditionMessage(error))
    structure(list(message=msg, original_error_class=class(error)),
              class=c("worker-comm-error", "bperror", "error", "condition"))
}

.error_bplist <- function(result) {
    idx <- which(!bpok(result))
    err <- structure(list(
        message=sprintf(
            "BiocParallel errors\n  element index: %s%s\n  first error: %s",
            paste(head(idx), collapse=", "),
            if (length(idx) > 6) ", ..." else "",
            conditionMessage(result[[idx[1]]]))),
        result=result,
        class = c("bplist-error", "bperror", "error", "condition"))
}

`print.remote-error` <- function(x, ...) {
    NextMethod(x)
    cat("traceback() available as 'attr(x, \"traceback\")'\n")
}

`print.bplist-error` <- function(x, ...) {
    NextMethod(x)
    cat("results and errors available as 'attr(x, \"result\")'\n")
}
