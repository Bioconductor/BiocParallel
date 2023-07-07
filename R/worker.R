### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utils

## Extract static and dynamic data from a task Return NULL if no
## static data can be extracted
.task_const <-
    function(value)
{
    ## Supports EXEC task only
    if (value$type != "EXEC")
        return(NULL)
    if (isTRUE(value$dynamic.only))
        return(NULL)

    if (value$static.fun)
        fun <- value$data$fun
    else
        fun <- NULL

    fullArgNames <- names(value$data$args)
    if (all(value$static.args %in% fullArgNames)) {
        args <- value$data$args[value$static.args]
        if (!length(args)) args <- NULL
    } else {
        args <- NULL
    }

    if (!is.null(fun) || !is.null(args))
        list(fun = fun, args = args, fullArgNames = fullArgNames)
    else
        NULL
}

## Extract the dynamic part from a task
.task_dynamic <-
    function(value)
{
    ## Supports EXEC task only
    if (value$type != "EXEC")
        return(value)

    if (value$static.fun)
        value$data$fun <- TRUE
    if (length(value$static.args))
        value$data$args[value$static.args] <- NULL

    if (value$static.fun || length(value$static.args))
        value$dynamic.only <- TRUE

    value
}

## Recreate the task from the dynamic and static parts of the task
## It is safe to call the function if the task is complete
## (Not extracted by `.task_dynamic`) or `static_Data` is NULL
.task_remake <-
    function(value, static_data = NULL)
{
    if (is.null(static_data))
        return(value)
    if (value$type != "EXEC")
        return(value)
    if (!isTRUE(value$dynamic.only))
        return(value)

    if (value$static.fun)
        value$data$fun <- static_data$fun
    if (length(value$static.args)) {
        value$data$args <- c(value$data$args, static_data$args)
        value$data$args <- value$data$args[static_data$fullArgNames]
    }
    value$dynamic.only <- NULL
    value
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Worker commands

### Support for SOCK, MPI and FORK connections.
### Derived from snow version 0.3-13 by Luke Tierney
### Derived from parallel version 2.16.0 by R Core Team

.EXEC <-
    function(tag, fun, args, static.fun = FALSE, static.args = NULL)
{
    list(
        type = "EXEC",
        data = list(tag = tag, fun = fun, args = args),
        static.fun = static.fun,
        static.args = static.args
    )
}

.VALUE <-
    function(tag, value, success, time, log, sout)
{
    list(
        type = "VALUE",
        tag = tag, value = value, success = success, time = time,
        log = log, sout = sout
    )
}

.DONE <-
    function()
{
    list(type = "DONE")
}


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Worker options and function to run the task
.workerOptions <-
    function(
        log = FALSE,
        threshold = "INFO",
        stop.on.error = TRUE,
        as.error = TRUE,
        timeout = WORKER_TIMEOUT,
        exportglobals = TRUE,
        force.GC = FALSE)
{
    force(log)
    force(threshold)
    force(stop.on.error)
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
            "topLevelEnvironment", "connectionObserver"
        )
        globalOptions <- base::options()
        globalOptions <- globalOptions[!names(globalOptions) %in% blocklist]
    } else {
        globalOptions <- NULL
    }

    list(
        log = log,
        threshold = threshold,
        stop.on.error = stop.on.error,
        as.error = as.error,
        timeout = timeout,
        force.GC = force.GC,
        globalOptions = globalOptions
    )
}

.composeTry <-
    function(FUN, OPTIONS, SEED)
{
    FUN <- match.fun(FUN)
    ERROR_OCCURRED <- FALSE
    ## use `ERROR_CALL_DEPTH` to trim call stack. default: show all
    ERROR_CALL_DEPTH <- -.Machine$integer.max
    UNEVALUATED <- .error_unevaluated() # singleton

    log <- OPTIONS$log
    stop.on.error <- OPTIONS$stop.on.error
    as.error <- OPTIONS$as.error
    timeout <- OPTIONS$timeout
    force.GC <- OPTIONS$force.GC
    globalOptions <- OPTIONS$globalOptions

    handle_warning <- function(w) {
        .log_warn(log, "%s", w)
        w       # FIXME: muffleWarning; don't rely on capture.output()
    }

    handle_error <- function(e) {
        ERROR_OCCURRED <<- TRUE
        .log_error(log, "%s", e)
        call <- rev(tail(sys.calls(), -ERROR_CALL_DEPTH))
        .error_remote(e, call)
    }

    if (!is.null(SEED))
        SEED <- .rng_reset_generator("L'Ecuyer-CMRG", SEED)$seed

    function(...) {
        if (!identical(timeout, WORKER_TIMEOUT)) {
            setTimeLimit(timeout, timeout, TRUE)
            on.exit(setTimeLimit(Inf, Inf, FALSE))
        }

        if (!is.null(globalOptions))
            base::options(globalOptions)

        if (stop.on.error && ERROR_OCCURRED) {
            UNEVALUATED
        } else {
            .rng_reset_generator("L'Ecuyer-CMRG", SEED)

            ## capture warnings and errors. Both are initially handled
            ## by `withCallingHandlers()`.
            ##
            ## 'error' conditions are logged (via `handle_error()`),
            ## annotated, and then re-signalled via `stop()`.  The
            ## condition needs to be handled first by
            ## `withCallingHandlers()` so that the full call stack to
            ## the error can be recovered.  The annotated condition
            ## needs to be resignalled so that it can be returned as
            ## 'output'; but the condition needs to be silenced by the
            ## outer `tryCatch()`.
            ##
            ## 'warning' conditions are logged (via
            ## `handle_warning()`). The handler returns the original
            ## condition, and the 'muffleWarning' handler is invoked
            ## somewhere above this point.
            output <- tryCatch({
                withCallingHandlers({
                    ## emulate call depth from 'inside' FUN, to
                    ## account for frames from tryCatch,
                    ## withCallingHandlers
                    ERROR_CALL_DEPTH <<- (\() sys.nframe() - 1L)()
                    FUN(...)
                }, error = function(e) {
                    annotated_condition <- handle_error(e)
                    stop(annotated_condition)
                }, warning = handle_warning)
            }, error = identity)

            ## Trigger garbage collection to cut down on memory usage within
            ## each worker in shared memory contexts. Otherwise, each worker is
            ## liable to think the entire heap is available (leading to each
            ## worker trying to fill said heap, causing R to exhaust memory).
            if (force.GC)
                gc(verbose=FALSE, full=FALSE)

            SEED <<- .rng_next_substream(SEED)

            output
        }
    }
}

.workerLapply_impl <-
    function(X, FUN, ARGS, OPTIONS, BPRNGSEED,
             GLOBALS = NULL, PACKAGES = NULL)
{
    state <- .rng_get_generator()
    on.exit(.rng_reset_generator(state$kind, state$seed))

    ## FUN is not compiled when using MulticoreParam
    FUN <- compiler::cmpfun(FUN)

    if (!is.null(OPTIONS$globalOptions)) {
        oldOptions <- base::options()
        on.exit(base::options(oldOptions), add = TRUE)
    }

    ## Set log
    .log_load(OPTIONS$log, OPTIONS$threshold)

    for (pkg in PACKAGES) {
        suppressPackageStartupMessages(library(pkg, character.only = TRUE))
    }
    ## Add variables to the global space and remove them afterward
    ## Recover the replaced variables at the end if necessary
    replaced_variables <- new.env(parent = emptyenv())
    if (length(GLOBALS)) {
        for (i in names(GLOBALS)) {
            if (exists(i, envir = .GlobalEnv))
                replaced_variables[[i]] <- .GlobalEnv[[i]]
            assign(i, GLOBALS[[i]], envir = .GlobalEnv)
        }
        on.exit({
            remove(list = names(GLOBALS), envir = .GlobalEnv)
            for (i in names(replaced_variables))
                assign(i, replaced_variables[[i]], envir = .GlobalEnv)
        },
        add = TRUE)
    }

    composeFunc <- .composeTry(FUN, OPTIONS, BPRNGSEED)
    args <- c(list(X = X, FUN = composeFunc), ARGS)
    do.call(lapply, args)
}

## reduce the size of the serialization of .workerLapply_impl from
## 124k to 3k
.workerLapply <- eval(
    parse(text = "function(...) BiocParallel:::.workerLapply_impl(...)"),
    envir = getNamespace("base")
)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Worker loop.  Error handling is done in .composeTry.

.bpworker_EXEC <-
    function(msg, sink.sout = TRUE)
{
    ## need local handler for worker read/send errors
    if (sink.sout) {
        on.exit({
            sink(NULL, type="message")
            sink(NULL, type="output")
            close(file)
        })
        file <- rawConnection(raw(), "r+")
        sink(file, type="message")
        sink(file, type="output")
    }

    t1 <- proc.time()
    value <- tryCatch({
        do.call(msg$data$fun, msg$data$args)
    }, error=function(e) {
        ## return as 'list()' because msg$fun has lapply semantics
        list(.error_worker_comm(e, "worker evaluation failed"))
    })
    t2 <- proc.time()

    if (sink.sout) {
        sout <- rawToChar(rawConnectionValue(file))
        if (!nchar(sout)) sout <- NULL
    } else {
        sout <- NULL
    }

    success <- !(inherits(value, "bperror") || !all(bpok(value)))
    log <- .log_buffer_get()
    ## Reset the log buffer
    .log_buffer_init()

    value <- .VALUE(
        msg$data$tag, value, success, t2 - t1, log, sout
    )
}

.bpworker_impl <-
    function(worker)
{
    repeat {
        tryCatch({
            msg <- .recv(worker)
            if (inherits(msg, "error"))
                ## FIXME: try to return error to manager
                break                   # lost socket connection?
            if (msg$type == "DONE") {
                .close(worker)
                break
            } else if (msg$type == "EXEC") {
                value <- .bpworker_EXEC(msg)
                .send(worker, value)
            }
        }, interrupt = function(e) {
            NULL
        })
    }
}
