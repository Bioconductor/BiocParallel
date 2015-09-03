### =========================================================================
### Error handling 
### -------------------------------------------------------------------------

bplasterror <- function() {
    .Deprecated(msg = "bplasterror has been deprecated")
}

bpresume <- function(expr) {
    .Deprecated(msg = "bpresume has been deprecated")
}

## .try() functions
.try <- function(expr) {
    handler_warning <- function(w) {
        w 
    }
    handler_error <- function(e) {
        success <<- FALSE
        call <- sapply(sys.calls(), deparse)
        e <- structure(e, 
                       class = c("remote-error", "condition"),
                       traceback = capture.output(traceback(call)))
        invokeRestart("abort", e)
    }
    handler_abort <- function(e) e 

    withRestarts(withCallingHandlers(expr,
                                     warning=handler_warning,
                                     error=handler_error),
                                     abort=handler_abort)
}

.try_log <- function(expr) {
    handler_warning = function(w) {
        flog.warn("%s", w)
    }
    handler_error = function(e) {
        success <<- FALSE
        call <- sapply(sys.calls(), deparse)
        flog.error("%s", e)
        e <- structure(e, 
                       class = c("remote-error", "condition"),
                       traceback = capture.output(traceback(call))) 
        invokeRestart("abort", e)
    }
    handler_abort = function(e) e 
    withRestarts(withCallingHandlers(expr, 
                                     warning=handler_warning, 
                                     error=handler_error), 
                                     abort=handler_abort)
}



.composeTry <- function(FUN, log=FALSE, timeout=Inf)
{
    FUN <- match.fun(FUN)
    if (log)
        logFUN <- .try_log
    else
        logFUN <- .try

    function(...) {
        setTimeLimit(timeout, timeout, TRUE)
        on.exit(setTimeLimit(Inf, Inf, FALSE))
        logFUN(FUN(...))
    }
}

`print.remote-error` = function(x, ...) {
    NextMethod(x)
    cat("traceback() available as 'attr(x, \"traceback\")'\n")
}
