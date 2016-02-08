### =========================================================================
### Low-level cluster utilities
### -------------------------------------------------------------------------

### Support for SOCK, MPI and FORK connections.
### Derived from snow version 0.3-13 by Luke Tierney
### Derived from parallel version 2.16.0 by R Core Team

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Worker loop used by SOCK, MPI and FORK.  Error handling is done in
### .composeTry.

bploop <- function(manager, ...)
    UseMethod("bploop")

.bploop.worker <- function(manager, ...)
{
    repeat {
        tryCatch({
            msg <- tryCatch({
                parallel:::recvData(manager)
            }, error=identity)
            if (inherits(msg, "simpleError"))
                ## lost socket connection?
                break

            if (msg$type == "DONE") {
                parallel:::closeNode(manager)
                break
            } else if (msg$type == "EXEC") {
                ## need local handler for worker read/send errors
                sout <- character()
                file <- textConnection("sout", "w", local=TRUE)
                sink(file, type="message")
                sink(file, type="output")
                gc(reset=TRUE)
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

                node <- Sys.info()["nodename"]
                success <- !(is(value, "error") ||
                             any(vapply(value, is, logical(1), "error")))
                value <- list(type = "VALUE", value = value,
                              success = success,
                              time = t2 - t1, tag = msg$data$tag,
                              log = .log_buffer_get(),
                              gc = gc(), node = node, sout = sout)

                parallel:::sendData(manager, value)
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
            d <- parallel:::recvOneData(cl)
            if (!is.null(result))
                result[[d$value$tag]] <- d$value$value
            running[d$node] <- FALSE
        }
    }, error=function(e) {
        stop(.error_worker_comm(e, "stop worker failed"))
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

        submit <- function(node, job) {
            parallel:::sendCall(cl[[node]], FUN, ARGFUN(job), tag = job)
            TRUE
        }

        ## initial load
        running <- logical(workers)
        for (i in seq_len(min(n, workers)))
            running[i] <- submit(i, i)

        for (i in seq_len(n)) {
            ## collect
            tryCatch({
                d <- parallel:::recvOneData(cl)
            }, error=function(e) {
                stop(.error_worker_comm(e, "bplapply receive data failed"))
            })

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
                running[d$node] <- submit(d$node, j)
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

    submit <- function(node, job, value) {
        parallel:::sendCall(cl[[node]], FUN, ARGFUN(value), tag = job)
        TRUE
    }


    ## initial load
    for (i in seq_len(workers)) {
        iter_value <- ITER()
        if (is.null(iter_value)) {
            if (i == 1L)
                warning("first invocation of 'ITER()' returned NULL")
            break
        }
        running[i] <- submit(i, i, iter_value)
    }

    repeat {
        if (!any(running))
            break

        ## collect
        d <- tryCatch({
            parallel:::recvOneData(cl)
        }, error=function(e) {
            stop(.error_worker_comm(e, "bpiterate receive data failed"))
        })

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
        iter_value <- ITER()
        if (!is.null(iter_value)) {
            i <- i + 1L
            running[d$node] <- submit(d$node, i, iter_value)
        }
    }

    ## return results
    if (!is.na(bpresultdir(BPPARAM))) {
        NULL
    } else {
        reducer$value()
    }
}
