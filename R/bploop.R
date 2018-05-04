### =========================================================================
### Low-level cluster utilities
### -------------------------------------------------------------------------

### Support for SOCK, MPI and FORK connections.
### Derived from snow version 0.3-13 by Luke Tierney
### Derived from parallel version 2.16.0 by R Core Team

.send_EXEC <-
    function(node, tag, fun, args)
{
    data <- list(type="EXEC", data=list(fun=fun, args=args, tag=tag))
    parallel:::sendData(node, data)
    TRUE
}

.send_VALUE <-
    function(node, tag, value, success, time, log, sout)
{
    data <- list(type = "VALUE", tag = tag,
                 value = value, success = success,
                 time = time, log = log, sout = sout)
    parallel:::sendData(node, data)
    TRUE
}

.recv <- function(node, id)
    tryCatch({
        suppressPackageStartupMessages({
            ## when starting workers; not supressable otherwise
            parallel:::recvData(node)
        })
    }, error=function(e) {
        ## capture without throwing
        .error_worker_comm(e,  sprintf("'%s' receive data failed", id))
    })

.recv1 <- function(cluster, id)
    tryCatch({
        parallel:::recvOneData(cluster)
    }, error=function(e) {
        stop(.error_worker_comm(e, sprintf("'%s' receive data failed", id)))
    })

.close <- function(node)
    parallel:::closeNode(node)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Worker loop used by SOCK, MPI and FORK.  Error handling is done in
### .composeTry.

bploop <- function(manager, ...)
    UseMethod("bploop")

.bploop.worker <- function(manager, ...)
{
    repeat {
        tryCatch({
            msg <- .recv(manager, "worker")
            if (inherits(msg, "error"))
                break                   # lost socket connection?

            if (msg$type == "DONE") {
                .close(manager)
                break
            } else if (msg$type == "EXEC") {
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

                .send_VALUE(manager, msg$data$tag, value, success, t2 - t1,
                            log, sout)
            }
        }, interrupt = function(e) {
            NULL
        })
    }
}

bploop.MPInode <- .bploop.worker
bploop.SOCKnode <- .bploop.worker
bploop.SOCK0node <- .bploop.worker

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
            d <- .recv1(cl, "clear_cluster")
            if (!is.null(result))
                result[[d$value$tag]] <- d$value$value
            running[d$node] <- FALSE
        }
    }, error=function(e) {
        warning(.error_worker_comm(e, "stop worker failed"))
    })
    result
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

bploop.lapply <-
    function(manager, X, FUN, ARGFUN, BPPARAM, ...)
{
    cl <- bpbackend(BPPARAM)

    n <- length(X)
    workers <- length(cl)
    result <- vector("list", n)

    if (n > 0 && workers > 0) {
        progress <- .progress(active=bpprogressbar(BPPARAM))
        on.exit(progress$term(), TRUE)
        progress$init(n)

        ## initial load
        running <- logical(workers)
        for (i in seq_len(min(n, workers)))
            running[i] <- .send_EXEC(cl[[i]], i, FUN, ARGFUN(i))

        for (i in seq_len(n)) {
            ## collect
            d <- .recv1(cl, "bplapply")

            value <- d$value$value
            njob <- d$value$tag
            result[[njob]] <- value
            running[d$node] <- FALSE

            progress$step()
            .manager_log(BPPARAM, njob, d)
            .manager_result_save(BPPARAM, njob, value)

            if (bpstopOnError(BPPARAM) && !d$value$success) {
                ## let running jobs finish, don't re-load
                result <- .clear_cluster(cl, running, result)
                break
            }

            ## re-load
            j <- i + min(n, workers)
            if (j <= n)
                running[d$node] <- .send_EXEC(cl[[d$node]], j, FUN, ARGFUN(j))
        }
    }

    ## return results
    if (!is.na(bpresultdir(BPPARAM)))
        NULL
    else result
}

##
## bploop.iterate():
##
## Derived from snow::dynamicClusterApply and parallel::mclapply.
##
## - length of 'X' is unknown (defined by ITER())
## - results not pre-allocated; list grows each iteration if no REDUCE
## - args wrapped in arglist with different chunks from ITER()
##

bploop.iterate <-
    function(manager, ITER, FUN, ARGFUN, BPPARAM, REDUCE, init, reduce.in.order,
             ...)
{
    cl <- bpbackend(BPPARAM)

    workers <- length(cl)
    running <- logical(workers)
    reducer <- .reducer(REDUCE, init, reduce.in.order)

    progress <- .progress(active=bpprogressbar(BPPARAM), iterate=TRUE)
    on.exit(progress$term(), TRUE)
    progress$init()

    ## initial load
    for (i in seq_len(workers)) {
        value <- ITER()
        if (is.null(value)) {
            if (i == 1L)
                warning("first invocation of 'ITER()' returned NULL")
            break
        }
        running[i] <- .send_EXEC(cl[[i]], i, FUN, ARGFUN(value))
    }

    repeat {
        if (!any(running))
            break

        ## collect
        d <- .recv1(cl, "bpiterate")
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
            .clear_cluster(cl, running)
            break
        }

        ## re-load
        value <- ITER()
        if (!is.null(value)) {
            i <- i + 1L
            running[d$node] <- .send_EXEC(cl[[d$node]], i, FUN, ARGFUN(value))
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

    message("Adding jobs ...")
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
        ids = ids, resources = list(), reg = BPPARAM$registry
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
