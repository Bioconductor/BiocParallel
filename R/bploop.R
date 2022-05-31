### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Manager loop used by SOCK, MPI and FORK

## collect the results from the workers
.collect_result <-
    function(manager, task_iterator, reducer, tasks_index, progress, BPPARAM)
{
    data_list <- .manager_recv(manager)
    success <- rep(TRUE, length(data_list))
    for(i in seq_along(data_list)){
        ## each result is a list containing the element value passed
        ## in `.send` and possibly other elements used by the backend
        d <- data_list[[i]]

        node <- d$node
        value <- d$value

        result <- value$value
        task_id <- value$tag
        time <- value$time

        ## obtain the indices of `X` for a task
        task_index <- tasks_index[[as.character(task_id)]]
        rm(list = as.character(task_id), envir = tasks_index)

        ## report time to the balancer
        task_iterator$record(node, task_id, time[["elapsed"]])

        ## reduce
        .reducer_add(reducer, task_index, result)
        .manager_log(BPPARAM, task_id, d)
        .manager_result_save(BPPARAM, task_id, reducer$value())

        ## progress
        progress$step(length(result))

        ## whether the result is ok, or to treat the failure as success
        success[i] <- !bpstopOnError(BPPARAM) || value$success
    }
    success
}

## These functions are used by all cluster types (SOCK, MPI, FORK) and
## run on the master. Both enable logging, writing logs/results to
## files and 'stop on error'.
.clear_cluster <-
    function(manager, running, task_iterator, reducer, tasks_index, progress, BPPARAM)
{
    tryCatch({
        setTimeLimit(30, 30, TRUE)
        on.exit(setTimeLimit(Inf, Inf, FALSE))
        while (running) {
            success <- .collect_result(manager, task_iterator, reducer, tasks_index,
                                       progress, BPPARAM)
            running <- running - length(success)
        }
    }, error=function(e) {
        message("Stop worker failed with the error: ", conditionMessage(e))
    })
    reducer
}

.manager_log <-
    function(BPPARAM, task_id, d)
{
    if (bplog(BPPARAM)) {
        con <- NULL
        if (!is.na(bplogdir(BPPARAM))) {
            fname <- paste0(bpjobname(BPPARAM), ".task", task_id, ".log")
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
    function(BPPARAM, task_id, value)
{
    if (is.na(bpresultdir(BPPARAM)))
        return(NULL)

    fname <- paste0(bpjobname(BPPARAM), ".task", task_id, ".Rda")
    rfile <- file.path(bpresultdir(BPPARAM), fname)
    save(value, file=rfile)
}

## A task generator for bploop.lapply
## nextTask: returns a list containing `index` and `seed`,
##           if no next task, return seed only
## record: record the execution time of a task
.bploop_lapply_task_iterator <-
    function(X, balancer, init_seed, redo_index, ntotal)
{
    seed_generator <- .seed_generator(init_seed)
    n <- length(redo_index)
    list(
        next_task = function() {
            if (n > 0) {
                task <- balancer$next_task()
                n <<- n - length(task$index)
                task$index <- redo_index[task$index]
                task$seed <- seed_generator(task$index)
                if (!is.null(task$index))
                    task$value <- X[task$index]
                task
            } else {
                list(seed = seed_generator(ntotal + 1L))
            }
        },
        record = function(node, task_id, time)
            balancer$record(node, task_id, time),
        seed = function() seed_generator(ntotal + 1L)
    )
}

## An iterator for bpiterate to ignore the finished task in BPREDO
.bploop_iterate_with_redo <-
    function(ITER, redo_index)
{
    redo_index_diff <- diff(c(0, redo_index))
    i <- 0L
    function(){
        if (i < length(redo_index)) {
            i <<- i + 1L
            for (j in seq_len(redo_index_diff[i]))
                value <- ITER()
            value
        } else {
            ITER()
        }
    }
}

.bploop_iterate_task_iterator <- function(balancer, redo_ITER,
                                 init_seed, redo_index){
    seed_generator <- .seed_generator(init_seed)
    task_original_index <- out_of_range_vector(redo_index)
    EOF <- FALSE
    ## The maximum observed seed index
    next_job_seed_index <- tail(redo_index, 1)
    list(
        next_task = function() {
            if (!EOF) {
                task <- balancer$next_task()
                if (length(task$value) != 0) {
                    ## The index can be out of range
                    if (tail(task$index, 1L) <= length(redo_index))
                        task$index <- redo_index[task$index]
                    else
                        task$index <- vapply(task$index ,task_original_index, numeric(1))
                    task$seed <- seed_generator(task$index)
                    next_job_seed_index <<-
                        max(next_job_seed_index, tail(task$index, 1L) + 1L)
                }
                ## Reach the end of the iterator
                if (is.null(task$value))
                    EOF <<- TRUE
                task
            } else {
                list(seed = seed_generator(next_job_seed_index))
            }
        },
        record = function(node, task_id, time)
            balancer$record(node, task_id, time),
        seed = function() seed_generator(next_job_seed_index)
    )
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
## - ITER: A list with two functions `nextTask` and `record`
##   nextTask(seedOnly): Return a list with the following elements
##      1. task_id: the id of the current task
##      2. index: The original position of the value(must be increasing)
##      3. value: a list of elements that need to be evaluated by FUN
##      4. seed: The initial seed used by the evaluation
##      If nothing to proceed, ITER should return a list
##      with the element seed only
##   record(task_id, time): report the task evaluation time
##
## - FUN: A function that will be evaluated in the worker
## - ARGS: the arguments to FUN
.bploop_impl <-
    function(task_iterator, FUN, ARGS, BPPARAM, BPREDO, BPOPTIONS,
             reducer, progress.length)
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

    ## set the last seed stream back to BPPARAM
    if (identical(BPREDO, list()))
        on.exit(.RNGstream(BPPARAM) <- task_iterator$seed(), add = TRUE)

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
    ARGFUN <- function(X, seed, index)
        list(
            X=X , FUN=FUN , ARGS = ARGS,
            OPTIONS = OPTIONS, BPRNGSEED = seed,
            INDEX = index,
            GLOBALS = globalVars,
            PACKAGES = packages
        )
    static.args <- c("FUN", "ARGS", "OPTIONS", "GLOBALS", "PACKAGES")

    ## Used as a map to store the indices of the vector `X` or ITER
    ## for a task
    tasks_index <- new.env(parent = emptyenv())

    total <- 0L
    running <- 0L
    task <- list(value = 0L)  ## an arbitrary initial
    ## keep the loop when there exists more tasks or running tasks
    while (!is.null(task$value) || running) {
        ## send tasks to the workers
        while (running < .manager_capacity(manager)) {
            task <- task_iterator$next_task()
            if (is.null(task$value)) {
                if (total == 0L)
                    warning("first invocation of 'ITER()' returned NULL")
                break
            }
            ## record the task index
            tasks_index[[as.character(task$task_id)]] <- task$index

            args <- ARGFUN(task$value, task$seed, task$index)
            exec <- .EXEC(
                tag = task$task_id,
                fun = .workerLapply,
                args = args,
                static.fun = TRUE,
                static.args = static.args
            )
            .manager_send(manager, exec)
            total <- total + 1L
            running <- running + 1L
        }
        .manager_flush(manager)

        ## If the cluster does not have any worker, waiting for the worker
        if (!running)
            next

        ## collect results from the workers
        success <- .collect_result(manager = manager,
                                   task_iterator = task_iterator,
                                   reducer = reducer,
                                   tasks_index = tasks_index,
                                   progress = progress,
                                   BPPARAM = BPPARAM)
        running <- running - length(success)

        ## stop on error; Let running jobs finish and break
        if (!all(success)) {
            reducer <- .clear_cluster(
                manager = manager,
                running = running,
                task_iterator = task_iterator,
                reducer = reducer,
                tasks_index = tasks_index,
                progress = progress,
                BPPARAM = BPPARAM
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
        .redo_seed(res) <- .RNGstream(BPPARAM)
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
    if (!identical(BPREDO, list())) {
        init_seed <- .redo_seed(BPREDO)
    } else {
        init_seed <- .RNGstream(BPPARAM)
    }
    ntotal <- length(X)
    nredo <- length(redo_index)

    balancer_generator <- .find_balancer("lapply", BPOPTIONS$lapplyBalancer)
    ## The balancer only need to give the redo index, so we pass
    ## nredo instead of ntotal
    balancer <- balancer_generator(nredo, BPPARAM)

    ## The iterator for .bploop_core
    task_iterator <- .bploop_lapply_task_iterator(
        X = X,
        balancer = balancer,
        init_seed = init_seed,
        redo_index = redo_index,
        ntotal = ntotal
    )
    reducer <- .lapply_reducer(ntotal, reducer = .redo_reducer(BPREDO))

    res <- .bploop_impl(
        task_iterator = task_iterator,
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
    redo_index <- .redo_index(ITER, BPREDO)
    if (!identical(BPREDO, list())) {
        init_seed <- .redo_seed(BPREDO)
    } else {
        init_seed <- .RNGstream(BPPARAM)
    }

    ## Ignore the task which do not need to be redone
    redo_ITER <- .bploop_iterate_with_redo(ITER, redo_index)

    ## Create balancer
    balancer_generator <- .find_balancer("iterate", BPOPTIONS$iterateBalancer)
    balancer <- balancer_generator(redo_ITER, BPPARAM)

    ## The iterator for .bploop_core
    task_iterator <- .bploop_iterate_task_iterator(
        balancer = balancer,
        redo_ITER = redo_ITER,
        init_seed = init_seed,
        redo_index = redo_index
    )
    reducer <- .iterate_reducer(
        REDUCE = REDUCE,
        init = init,
        reduce.in.order = reduce.in.order,
        reducer = .redo_reducer(BPREDO))

    .bploop_impl(
        task_iterator = task_iterator,
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
    reducer <- .iterate_reducer(REDUCE, init, reduce.in.order,
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
