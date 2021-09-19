### =========================================================================
### Low-level cluster utilities
### -------------------------------------------------------------------------

### Support for SOCK, MPI and FORK connections.
### Derived from snow version 0.3-13 by Luke Tierney
### Derived from parallel version 2.16.0 by R Core Team

.EXEC <-
    function(tag, fun, args)
{
    list(type="EXEC", data=list(fun=fun, args=args, tag=tag))
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
### Worker loop used by SOCK, MPI and FORK.  Error handling is done in
### .composeTry.

.bpworker_EXEC <- function(msg)
{
    ## need local handler for worker read/send errors
    sout <- character()
    file <- textConnection("sout", "w", local=TRUE)
    sink(file, type="message")
    sink(file, type="output")
    t1 <- proc.time()
    value <- tryCatch({
        do.call(msg$data$fun, msg$data$args)
    }, error=function(e) {
        ## capture error, without throwing
        .error_worker_comm(e, "worker evaluation failed")
    })
    t2 <- proc.time()
    sink(NULL, type="message")
    sink(NULL, type="output")
    close(file)

    success <- !(inherits(value, "bperror") || !all(bpok(value)))

    log <- .log_buffer_get()

    value <- .VALUE(
        msg$data$tag, value, success, t2 - t1, log, sout
    )
}

.bpworker_impl <- function(worker)
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

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Manager loop used by SOCK, MPI and FORK

## These functions are used by all cluster types (SOCK, MPI, FORK) and
## run on the master. Both enable logging, writing logs/results to
## files and 'stop on error'. The 'cl' argument is an active cluster;
## starting and stopping of 'cl' is done outside these functions, eg,
## bplapply, bpmapply etc..

.clear_cluster <- function(cl, running, result=NULL)
{
    tryCatch({
        setTimeLimit(30, 30, TRUE)
        on.exit(setTimeLimit(Inf, Inf, FALSE))
        while (any(running)) {
            d <- .recv_any(cl)
            if (!is.null(result))
                result[[d$value$tag]] <- d$value$value
            running[d$node] <- FALSE
        }
    }, error=function(e) {
        warning(.error_worker_comm(e, "stop worker failed"))
    })
    result
}

.wait_results <- function(cl, running, reducer)
{
    tryCatch({
        setTimeLimit(30, 30, TRUE)
        on.exit(setTimeLimit(Inf, Inf, FALSE))
        while (any(running)) {
            d <- .recv_any(cl)
            njob <- d$value$tag
            value <- d$value$value
            reducer$add(njob, value)
            running[d$node] <- FALSE
        }
    }, error=function(e) {
        warning(.error_worker_comm(e, "stop worker failed"))
    })
    reducer
}

.manager_log <- function(BPPARAM, njob, d) {
    if (bplog(BPPARAM)) {
        con <- NULL
        if (!is.na(bplogdir(BPPARAM))) {
            fname <- paste0(bpjobname(BPPARAM), ".task", njob, ".log")
            lfile <- file.path(bplogdir(BPPARAM), fname)
            con <- file(lfile, open="a")
            on.exit(close(con))
        }
        .bpwriteLog(con, d)
    } else if (length(d$value$sout)) {
        message(paste(d$value$sout, collapse="\n"))
    }
}

.manager_result_save <- function(BPPARAM, njob, value) {
    if (is.na(bpresultdir(BPPARAM)))
        return(NULL)

    fname <- paste0(bpjobname(BPPARAM), ".task", njob, ".Rda")
    rfile <- file.path(bpresultdir(BPPARAM), fname)
    save(value, file=rfile)
}

.reducer <-
    function(REDUCE, init, reduce.in.order=FALSE)
{
    env <- new.env(parent=emptyenv())
    env[["reduce"]] <- !missing(REDUCE)
    env[["init"]] <- !missing(init)

    if (env[["reduce"]])
        env[["value"]] <- if (env[["init"]]) init else list()
    env[["index"]] <- 1L

    reduce_one <- function(idx) {
        if (env[["reduce"]]) {
            env[["value"]] <- if (!env[["init"]] && (env[["index"]] == 1L)) {
                env[[idx]]
            } else {
                REDUCE(env[["value"]], env[[idx]])
            }
            rm(list=idx, envir=env)
        }
        env[["index"]] <- env[["index"]] + 1L
        1L
    }

    add_immediate <- function(i, value) {
        env[[as.character(i)]] <- value
        reduce_one(as.character(i))
    }

    add_inorder <- function(i, value) {
        status <- 0L
        env[[as.character(i)]] <- value
        while (exists(as.character(env[["index"]]), envir=env))
            status <- status + reduce_one(as.character(env[["index"]]))
        status
    }

    list(add=function(i, value) {
        if (reduce.in.order)
            add_inorder(i, value)
        else add_immediate(i, value)
    }, isComplete=function() {
        length(setdiff(ls(env), c("reduce", "init", "value", "index"))) == 0L
    }, value=function() {
        if (env[["reduce"]])
            env[["value"]]              # NULL if no values
        else {
            lst <- as.list(env)
            idx <- setdiff(names(lst), c("reduce", "init", "value", "index"))
            unname(lst[idx[order(as.integer(idx))]])
        }
    })
}


##
## bploop.lapply(): derived from snow::dynamicClusterApply.
##
bploop <- function(manager, ...)
    UseMethod("bploop")


bploop.lapply <-
    function(manager, X, FUN, ARGS, BPPARAM)
{
        ITER <- .dummy_iter(X)
        manager <- structure(list(), class="iterate") # dispatch
        bploop(
            manager = manager,
            ITER = ITER,
            FUN = FUN,
            ARGS = ARGS,
            BPPARAM =BPPARAM,
            REDUCE = c,
            reduce.in.order = TRUE)
}


##
## bploop.iterate():
##
## Derived from snow::dynamicClusterApply and parallel::mclapply.
##
## - length of 'X' is unknown (defined by ITER())
## - results not pre-allocated; list grows each iteration if no REDUCE
## - args wrapped in arglist with different chunks from ITER()
## Arguments
## - ITER: Return a list where each list element will be passed to FUN,
##   if nothing to proceed, it should return list(NULL)
## - FUN: A function that accepts a scalar X
## - REDUCE(x, y): combine x and y where y is a list with each element
##   returned by FUN
bploop.iterate <-
    function(manager, ITER, FUN, ARGS, BPPARAM, REDUCE, init, reduce.in.order)
{
    cl <- bpbackend(BPPARAM)

    seed <- .bpnextRNGstream(BPPARAM)
    # seed <- .rng_next_substream(seed)

    workers <- length(cl)
    running <- logical(workers)
    reducer <- .reducer(REDUCE, init, reduce.in.order)

    progress <- .progress(active=bpprogressbar(BPPARAM), iterate=TRUE)
    on.exit(progress$term(), TRUE)
    progress$init()

    ARGFUN <- function(X, seed)
        c(
            list(X=X), list(FUN=FUN), ARGS,
            list(BPRNGSEED = seed)
        )
    ## initial load
    for (i in seq_len(workers)) {
        value <- ITER()
        if (inherits(value, "rng_iter")) {
            seed <- .rng_iterate_stream(seed, value)
            value <- ITER()
        }
        if (is.null(value[[1]])) {
            if (i == 1L)
                warning("first invocation of 'ITER()' returned NULL")
            break
        }
        value_ <- .EXEC(i, .rng_lapply, ARGFUN(value, seed))
        running[i] <- .send_to(cl, i, value_)
        seed <- .rng_iterate_stream(seed, length(value))
    }

    repeat {
        if (!any(running))
            break

        ## collect
        d <- .recv_any(cl)
        progress$step()

        value <- d$value$value
        njob <- d$value$tag
        running[d$node] <- FALSE

        ## reduce
        ## FIXME: if any worker has an error - can't reduce
        reducer$add(njob, value)
        .manager_log(BPPARAM, njob, d)
        .manager_result_save(BPPARAM, njob, reducer$value())

        if (bpstopOnError(BPPARAM) && !d$value$success) {
            ## stop on error; let running jobs finish, do not re-load
            ## FIXME: harvest assigned jobs
            reducer <- .wait_results(cl, running, reducer)
            break
        }

        ## re-load
        value <- ITER()
        if (inherits(value, "rng_iter")) {
            seed <- .rng_iterate_stream(seed, value)
            value <- ITER()
        }
        if (!is.null(value[[1]])) {
            i <- i + 1L
            value_ <- .EXEC(i, .rng_lapply, ARGFUN(value, seed))
            running[d$node] <- .send_to(cl, d$node, value_)
            seed <- .rng_iterate_stream(seed, length(value))
        }
    }

    ## return results
    if (!is.na(bpresultdir(BPPARAM))) {
        NULL
    } else {
        reducer$value()
    }
}

bploop.iterate_batchtools <-
    function(manager, ITER, FUN, BPPARAM, REDUCE, init, reduce.in.order,...)
{
    ## get number of workers
    workers <- bpnworkers(BPPARAM)
    ## reduce in order
    reducer <- .reducer(REDUCE, init, reduce.in.order)

    ## progress bar.
    progress <- .progress(active=bpprogressbar(BPPARAM), iterate=TRUE)
    on.exit(progress$term(), TRUE)
    progress$init()

    def.id <- job.id <- 1L
    repeat{
        value <- ITER()
        if (is.null(value)) {
            if (job.id == 1L)
                warning("first invocation of 'ITER()' returned NULL")
            break
        }

        ## save 'value' to registry tempfile
        fl <- tempfile(tmpdir = BPPARAM$registry$file.dir)
        saveRDS(value, fl)

        if (job.id == 1L) {
            suppressMessages({
                ids <- batchtools::batchMap(
                    fun = FUN, fl, more.args = list(...),
                    reg = BPPARAM$registry
                )
            })
        } else {
            job.pars <- list(fl)
            BPPARAM$registry$defs <-
                rbind(BPPARAM$registry$defs, list(def.id, list(job.pars)))
            entry <- c(list(job.id, def.id), rep(NA, 10))
            BPPARAM$registry$status <- rbind(BPPARAM$registry$status, entry)
        }
        def.id <- def.id + 1L
        job.id <- job.id + 1L
    }

    ## finish  updating tables
    ids <- data.table::data.table(job.id = seq_len(job.id - 1))
    data.table::setkey(BPPARAM$registry$status, "job.id")
    ids$chunk = batchtools::chunk(ids$job.id, n.chunks = workers)

    ## submit and wait for jobs
    batchtools::submitJobs(
        ids = ids, resources = .bpresources(BPPARAM), reg = BPPARAM$registry
    )
    batchtools::waitForJobs(
        ids = BPPARAM$registry$status$job.id,
        reg = BPPARAM$registry, timeout = bptimeout(BPPARAM),
        stop.on.error = bpstopOnError(BPPARAM)
    )

    ## reduce in order
    for (job.id in ids$job.id) {
        value <- batchtools::loadResult(id = job.id, reg=BPPARAM$registry)
        reducer$add(job.id, value)
    }

    ## return reducer value
    reducer$value()
}
