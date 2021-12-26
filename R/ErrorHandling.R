### =========================================================================
### Error handling
### -------------------------------------------------------------------------
.bpeltok <-
    function(x, type = bperrorTypes())
{
    !inherits(x, type)
}

bpok <-
    function(x, type = bperrorTypes())
{
    x <- bpresult(x)
    type <- match.arg(type)
    vapply(x, .bpeltok, logical(1), type)
}

.bpallok <-
    function(x, type = bperrorTypes(), attrOnly = FALSE)
{
    if (attrOnly)
        is.null(.redo_env(x))
    else
        is.null(.redo_env(x)) && all(bpok(x, type))
}

bptry <-
    function(expr, ..., bplist_error, bperror)
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
    if (is.null(attr(result, "errors"))) {
        errors <- result
        total_error <- sum(!bpok(errors))
        remote_error <-
            !bpok(errors, "remote_error") |
            !bpok(errors, "worker_comm_error")
        remote_idx <- which(remote_error)
        if (length(remote_idx))
          first_error <- errors[[remote_idx[1]]]
        else
          first_error <- ""
    } else {
        errors <- attr(result, "errors")
        total_error <- length(errors)
        remote_error <-
            !bpok(errors, "remote_error") |
            !bpok(errors, "worker_comm_error")
        first_error_idx <- which(remote_error)[1]
        if (!is.null(first_error_idx))
          first_error <- errors[[first_error_idx]]
        else
          first_error <- ""
        remote_idx <- as.integer(names(errors[remote_error]))
    }

    n_remote_error <- length(remote_idx)
    n_other_error <- total_error - n_remote_error

    fmt = paste(
        "BiocParallel errors",
        "%d remote errors, element index: %s%s",
        "%d unevaluated and other errors",
        "first remote error:\n%s",
        sep = "\n  "
    )
    class(first_error) <- tail(class(first_error), 2L)
    first_error_msg <- as.character(first_error)
    message <- sprintf(
        fmt,
        n_remote_error,
        paste(head(remote_idx), collapse = ", "),
        ifelse(length(remote_idx) > 6, ", ...", ""),
        n_other_error,
        first_error_msg
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
