### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Manager loop used by SOCK, MPI and FORK

## collect the results from the workers
.collect_result <-
    function(manager, reducer, progress, BPPARAM)
{
    data_list <- .manager_recv(manager)
    success <- rep(TRUE, length(data_list))
    for(i in seq_along(data_list)){
        ## each result is a list containing the element value passed
        ## in `.send` and possibly other elements used by the backend
        d <- data_list[[i]]

        value <- d$value$value
        njob <- d$value$tag

        ## reduce
        .reducer_add(reducer, njob, value)
        .manager_log(BPPARAM, njob, d)
        .manager_result_save(BPPARAM, njob, reducer$value())

        ## progress
        progress$step(length(value))

        ## whether the result is ok, or to treat the failure as success
        success[i] <- !bpstopOnError(BPPARAM) || d$value$success
    }
    success
}

## These functions are used by all cluster types (SOCK, MPI, FORK) and
## run on the master. Both enable logging, writing logs/results to
## files and 'stop on error'.
.clear_cluster <-
    function(manager, running, reducer, progress, BPPARAM)
{
    tryCatch({
        setTimeLimit(30, 30, TRUE)
        on.exit(setTimeLimit(Inf, Inf, FALSE))
        while (running) {
            success <- .collect_result(manager, reducer, progress, BPPARAM)
            running <- running - length(success)
        }
    }, error=function(e) {
        message("Stop worker failed with the error: ", conditionMessage(e))
    })
    reducer
}

.manager_log <-
    function(BPPARAM, njob, d)
{
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

.manager_result_save <-
    function(BPPARAM, njob, value)
{
    if (is.na(bpresultdir(BPPARAM)))
        return(NULL)

    fname <- paste0(bpjobname(BPPARAM), ".task", njob, ".Rda")
    rfile <- file.path(bpresultdir(BPPARAM), fname)
    save(value, file=rfile)
}


## A dummy iterator for bploop.lapply
.bploop_lapply_iter <-
    function(X, redo_index, elements_per_task)
{
    redo_n <- length(redo_index)
    redo_i <- 1L
    x_n <- length(X)
    x_i <- 1L
    function() {
        if (redo_i <= redo_n && x_i <= x_n) {
            redo <- redo_index[redo_i] == x_i
            if (redo) {
                ## Maximize `len` such that
                ## - 1. all elements in X[x_i:(x_i + len)] should be redone
                ## - 2. the number of elements in the task must be
                ##      limited by `elements_per_task`
                len <- 1L
                while (redo_i + len <= redo_n &&
                       redo_index[redo_i + len] == x_i + len &&
                       len < elements_per_task) {
                    len <- len + 1L
                }
                redo_i <<- redo_i + len
                value <- X[seq.int(x_i, length.out = len)]
            } else {
                len <- redo_index[redo_i] - x_i
                value <- .bploop_rng_iter(len)
            }
            x_i <<- x_i + len
            ## Do not return the last seed iterator
            ## if no more redo element
            if (x_i > x_n && !redo) {
                list(NULL)
            } else {
                value
            }
        } else {
            list(NULL)
        }
    }
}

## An iterator for bpiterate to handle BPREDO
.bploop_iterate_iter <-
    function(ITER, reducer)
{
    errors <- sort(.redo_index_iterate(reducer))
    len <- reducer$total
    if(is.null(len)) len <- 0L
    i <- 0L
    function(){
        if (i < len) {
            i <<- i + 1L
            value <- ITER()
            if (i%in%errors)
                list(value)
            else
                .bploop_rng_iter(1L)
        } else {
            list(ITER())
        }
    }
}


## This class object can force bploop.iterator to iterate
## the seed stream n times
.bploop_rng_iter <- function(n) {
    structure(as.integer(n), class = c(".bploop_rng_iter"))
}

## Accessor for the elements in the BPREDO argument
## Return NULL if not exists
.redo_env <-
    function(x)
{
    attr(x, "REDOENV")
}

.redo_reducer <-
    function(x)
{
    .redo_env(x)$reducer
}

.redo_seed <-
    function(x)
{
    .redo_env(x)$rng_seed
}

`.redo_env<-` <-
    function(x, value)
{
    attr(x, "REDOENV") <- value
    x
}

`.redo_reducer<-` <-
    function(x, value)
{
    .redo_env(x)$reducer <- value
    x
}

`.redo_seed<-` <-
    function(x, value)
{
    .redo_env(x)$rng_seed <- value
    x
}

## The core bploop implementation
## Arguments
## - ITER: Return a list where each list element will be passed to FUN
##   1. if nothing to proceed, it should return list(NULL)
##   2. if the task is to iterate the seed stream only, it should return
##      an object from .bploop_rng_iter()
## - FUN: A function that will be evaluated in the worker
## - ARGS: the arguments to FUN
.bploop_impl <-
    function(ITER, FUN, ARGS, BPPARAM, BPREDO, BPOPTIONS, reducer, progress.length)
{
    manager <- .manager(BPPARAM)
    on.exit(.manager_cleanup(manager), add = TRUE)

    ## worker options
    OPTIONS <- .workerOptions(
        log = bplog(BPPARAM),
        threshold = bpthreshold(BPPARAM),
        stop.on.error = bpstopOnError(BPPARAM),
        timeout = bptimeout(BPPARAM),
        exportglobals = bpexportglobals(BPPARAM),
        force.GC = bpforceGC(BPPARAM)
    )

    ## prepare the seed stream for the worker
    init_seed <- .redo_seed(BPREDO)
    if (is.null(init_seed)) {
        seed <- .RNGstream(BPPARAM)
        on.exit(.RNGstream(BPPARAM) <- seed, add = TRUE)
        init_seed <- seed
    } else {
        seed <- init_seed
    }

    ## Progress bar
    progress <- .progress(
        active=bpprogressbar(BPPARAM), iterate=missing(progress.length)
    )
    on.exit(progress$term(), add = TRUE)
    progress$init(progress.length)

    ## detect auto export variables and packages
    globalVarNames <- as.character(BPOPTIONS$exports)
    packages <- as.character(BPOPTIONS$packages)
    if (bpexportvariables(BPPARAM)) {
        exports <- .findVariables(FUN)
        globalVarNames <- c(globalVarNames, exports$globalvars)
        packages <- c(packages, exports$pkgs)
    }
    globalVars <- lapply(globalVarNames, get, envir = .GlobalEnv)
    names(globalVars) <- globalVarNames

    ## The data that will be sent to the worker
    ARGFUN <- function(X, seed)
        list(
            X=X , FUN=FUN , ARGS = ARGS,
            OPTIONS = OPTIONS, BPRNGSEED = seed,
            GLOBALS = globalVars,
            PACKAGES = packages
        )
    static.args <- c("FUN", "ARGS", "OPTIONS", "GLOBALS")

    total <- 0L
    running <- 0L
    value <- NULL
    ## keep the loop when there exists more ITER value or running tasks
    while (!identical(value, list(NULL)) || running) {
        ## send tasks to the workers
        while (running < .manager_capacity(manager)) {
            value <- ITER()
            ## If the value is of the class .bploop_rng_iter, we merely iterate
            ## the seed stream `value` times and obtain the next value.
            if (inherits(value, ".bploop_rng_iter")) {
                seed <- .rng_iterate_substream(seed, value)
                next
            }
            if (identical(value, list(NULL))) {
                if (total == 0L)
                    warning("first invocation of 'ITER()' returned NULL")
                break
            }
            args <- ARGFUN(value, seed)
            task <- .EXEC(
                total + 1L, .workerLapply,
                args = args,
                static.fun = TRUE,
                static.args = static.args
            )
            .manager_send(manager, task)
            seed <- .rng_iterate_substream(seed, length(value))
            total <- total + 1L
            running <- running + 1L
        }
        .manager_flush(manager)

        ## If the cluster does not have any worker, waiting for the worker
        if (!running)
            next

        ## collect results from the workers
        success <- .collect_result(manager, reducer, progress, BPPARAM)
        running <- running - length(success)

        ## stop on error; Let running jobs finish and break
        if (!all(success)) {
            reducer <- .clear_cluster(
                manager, running, reducer, progress, BPPARAM
            )
            break
        }
    }

    ## return results
    if (!is.na(bpresultdir(BPPARAM)))
        return(NULL)

    res <- .reducer_value(reducer)
    ## Attach the redo information when the error occurs
    if(!.reducer_ok(reducer) || !.reducer_complete(reducer)) {
        .redo_env(res) <- new.env(parent = emptyenv())
        .redo_reducer(res) <- reducer
        .redo_seed(res) <- init_seed
    }
    res
}


##
## bploop.lapply(): derived from snow::dynamicClusterApply.
##
bploop <-
    function(manager, ...)
{
    UseMethod("bploop")
}

## X: the loop value after division
## ARGS: The function arguments for `FUN`
bploop.lapply <-
    function(manager, X, FUN, ARGS, BPPARAM,
             BPOPTIONS = bpoptions(), BPREDO = list(), ...)
{
    ## which need to be redone?
    redo_index <- .redo_index(X, BPREDO)

    ## How many elements in a task?
    ntask <- .ntask(X, bpnworkers(BPPARAM), bptasks(BPPARAM))
    elements_per_task <- ceiling(length(redo_index)/ntask)
    ITER <- .bploop_lapply_iter(X, redo_index, elements_per_task)

    ntotal <- length(X)
    reducer <- .lapplyReducer(ntotal, reducer = .redo_reducer(BPREDO))

    res <- .bploop_impl(
        ITER = ITER,
        FUN = FUN,
        ARGS = ARGS,
        BPPARAM = BPPARAM,
        BPOPTIONS = BPOPTIONS,
        BPREDO = BPREDO,
        reducer = reducer,
        progress.length = length(redo_index)
    )

    if (!is.null(res))
        names(res) <- names(X)

    res
}

##
## bploop.iterate():
##
## Derived from snow::dynamicClusterApply and parallel::mclapply.
##
## - length of 'X' is unknown (defined by ITER())
## - results not pre-allocated; list grows each iteration if no REDUCE
bploop.iterate <-
    function(
        manager, ITER, FUN, ARGS, BPPARAM,
        BPOPTIONS = bpoptions(), REDUCE, BPREDO,
        init, reduce.in.order, ...
    )
{
    ITER_ <- .bploop_iterate_iter(ITER, reducer = .redo_reducer(BPREDO))
    reducer <- .iterateReducer(REDUCE, init, reduce.in.order,
                               reducer = .redo_reducer(BPREDO))
    .bploop_impl(
        ITER = ITER_,
        FUN = FUN,
        ARGS = ARGS,
        BPPARAM = BPPARAM,
        BPOPTIONS = BPOPTIONS,
        BPREDO = BPREDO,
        reducer = reducer
    )
}

bploop.iterate_batchtools <-
    function(manager, ITER, FUN, BPPARAM, REDUCE, init, reduce.in.order, ...)
{
    ## get number of workers
    workers <- bpnworkers(BPPARAM)
    ## reduce in order
    reducer <- .iterateReducer(REDUCE, init, reduce.in.order,
                               NULL)

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
        reg = BPPARAM$registry, timeout = .batch_bptimeout(BPPARAM),
        stop.on.error = bpstopOnError(BPPARAM)
    )

    ## reduce in order
    for (job.id in ids$job.id) {
        value <- batchtools::loadResult(id = job.id, reg=BPPARAM$registry)
        .reducer_add(reducer, job.id, list(value))
    }

    ## return reducer value
    .reducer_value(reducer)
}
