### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utils
## Extract the unchanged part from a task
.task_type <-
    function(value)
{
    value$type
}

.EXEC_static <-
    function(value)
{
    if (value$static.fun)
        fun <- value$data$fun
    else
        fun <- NULL

    if (all(value$static.args %in% names(value$data$args))) {
        args <- value$data$args[value$static.args]
        if (!length(args)) args <- NULL
    } else {
        args <- NULL
    }

    if (!is.null(fun) || !is.null(args))
        list(fun = fun, args = args)
    else
        NULL
}

## Extract the dynamic part from a task
.EXEC_dynamic <-
    function(value)
{
    if (value$static.fun)
        value$data$fun <- NULL
    if (length(value$static.args))
        value$data$args[value$static.args] <- NULL

    value$dynamic.only <- TRUE
    value
}

## Recreate the task from the dynamic and static parts of EXEC.
## It is safe to call the function if EXEC is complete
## (Not extracted by `.EXEC_dynamic`).
.remake_EXEC <-
    function(value, static_EXEC = NULL)
{
    if (isTRUE(value$dynamic.only) && !is.null(static_EXEC)) {
        if (value$static.fun)
            value$data$fun <- static_EXEC$fun
        if (length(value$static.args))
            value$data$args <- c(value$data$args, static_EXEC$args)
    }
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
    list(type = "EXEC", data = list(tag = tag,
         fun = fun, args = args),
         static.fun = static.fun,
         static.args = static.args)
}

.VALUE <-
    function(tag, value, success, time, log, sout)
{
    list(type = "VALUE", tag = tag, value = value, success = success,
         time = time, log = log, sout = sout)
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
        stop.on.error = TRUE,
        as.error = TRUE,        # FALSE for BatchJobs compatible
        timeout = .Machine$integer.max,
        exportglobals = TRUE,
        force.GC = FALSE)
{
    force(log)
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
            "topLevelEnvironment"
        )
        globalOptions <- base::options()
        globalOptions <- globalOptions[!names(globalOptions) %in% blocklist]
    } else {
        globalOptions <- NULL
    }

    list(
        log = log,
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
    UNEVALUATED <- .error_unevaluated() # singleton

    handle_warning <- function(w) {
        .log_warn(log, "%s", w)
        w       # FIXME: muffleWarning; don't rely on capture.output()
    }

    handle_error <- function(e) {
        ERROR_OCCURRED <<- TRUE
        .log_error(log, "%s", e)
        call <- sapply(sys.calls(), deparse, nlines=3)
        if (OPTIONS$as.error) {
            .error_remote(e, call)
        } else {
            .condition_remote(e, call) # BatchJobs
        }
    }

    log <- OPTIONS$log
    stop.on.error <- OPTIONS$stop.on.error
    as.error <- OPTIONS$as.error
    timeout <- OPTIONS$timeout
    force.GC <- OPTIONS$force.GC
    globalOptions <- OPTIONS$globalOptions

    if (!is.null(SEED))
        SEED <- .rng_reset_generator("L'Ecuyer-CMRG", SEED)$seed

    function(...) {
        setTimeLimit(timeout, timeout, TRUE)
        on.exit(setTimeLimit(Inf, Inf, FALSE))

        if (!is.null(globalOptions)) {
            base::options(globalOptions)
        }

        if (stop.on.error && ERROR_OCCURRED) {
            UNEVALUATED
        } else {
            .rng_reset_generator("L'Ecuyer-CMRG", SEED)

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

            SEED <<- .rng_next_substream(SEED)

            output
        }
    }
}

.workerLapply_impl <-
    function(X, FUN, ARGS, OPTIONS, BPRNGSEED)
{
    state <- .rng_get_generator()
    on.exit(.rng_reset_generator(state$kind, state$seed))
    ## FUN is not compiled when using MulticoreParam
    FUN <- compiler::cmpfun(FUN)
    composeFunc <- .composeTry(FUN, OPTIONS, BPRNGSEED)
    args <- c(list(X = X, FUN = composeFunc), ARGS)
    do.call(lapply, args)
}

.workerLapply <- funcFactory("BiocParallel:::.workerLapply_impl")

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
