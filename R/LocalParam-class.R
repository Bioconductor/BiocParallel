##
## local_worker
##

setOldClass("local_worker")

local_worker <-
    function(path, timeout = 30L)
{
    mode <- "w+b"
    .Call(.local_worker, path, mode, timeout);
}

setMethod(
    ".recv", "local_worker",
    function(worker)
{
    res <- tryCatch({
        unserialize(worker)
    }, error = function(e) {
        .error_worker_comm(e, "'.recv,local_worker-method' failed")
    })
})

setMethod(
    ".send", "local_worker",
    function(worker, value)
{
    serialize(value, worker, xdr = FALSE)
})

setMethod(
    ".close", "local_worker",
    function(worker)
{
    close(worker)                       # close.connection
})

##
## local_manager
##

setOldClass("local_manager")

local_manager <-
    function(path, timeout = 30L, backlog = 5L)
{
    mode <- "w+b"
    .Call(.local_manager, path, mode, timeout, backlog)
}

local_manager_accept <-
    function(srv)
{
    .Call(.local_manager_accept, srv)
}

local_manager_selectfd <-
    function(srv, mode = c("r", "w"))
{
    mode <- match.arg(mode)
    .Call(.local_manager_selectfd, srv, mode)
}

local_manager_activefds <-
    function(srv)
{
    .Call(.local_manager_activefds, srv);
}

local_manager_set_activefd <-
    function(srv, fd)
{
    .Call(.local_manager_set_activefd, srv, fd)
}

send_to_local_manager <-
    function(x, node, value)
{
    fd <- local_manager_activefds(x)[node]
    local_manager_set_activefd(x, fd)
    serialize(value, x, xdr = FALSE)
}


recv_any_local_manager <-
    function(x)
{
    fd <- local_manager_selectfd(x)
    length(fd) || stop("'recv_any()' timeout")

    fd <- fd[sample.int(length(fd), 1L)]

    local_manager_set_activefd(x, fd)
    value <- unserialize(x)

    node <- match(fd, local_manager_activefds(x))
    structure(list(value = value, node = node), class = "recv_any")
}

isup_local_manager <-
    function(x)
{
    status <- tryCatch(summary(x)$opened, error = function(...) NULL)
    identical(status, "opened")
}

##
## local_cluster
##

setOldClass("local_cluster")

local_cluster <-
    function(n, timeout = 30L * 24L * 60L * 60L,
             worker, worker_id)
{
    fields <- list(
        con = NULL, n = n, timeout = timeout,
        worker = worker, worker_id = worker_id
    )
    env <- new.env(parent = emptyenv())
    structure(
        list2env(fields, env),
        class = "local_cluster"
    )
}

.con <- function(x)
    x$con

`.con<-` <-
    function(x, value)
{
    x$con <- value
}

open_local_cluster <-
    function(x)
{
    stopifnot(!isup_local_manager(.con(x)))

    path <- tempfile(fileext = ".skt")
    n <- x$n
    timeout <- x$timeout

    x$con <- local_manager(path, timeout = timeout, backlog = min(n, 128L))
    open(.con(x), "w+b")                # open.connection

    while (n > 0L) {
        n0 <- min(n, 128L)              # maximum backlog 128
        n <- n - n0
        replicate(n0, x$worker(path), simplify = FALSE)
        replicate(n0, local_manager_accept(.con(x)), simplify = FALSE)
    }

    invisible(x)
}

close_local_cluster <-
    function(x)
{
    if (isup_local_manager(.con(x))) {
        close(.con(x))                  # close.connection
        .con(x) <- NULL
    }
    invisible(x)
}

setMethod(
    ".send_to", "local_cluster",
    function(backend, node, value)
{
    send_to_local_manager(.con(backend), node, value)
    TRUE
})

setMethod(
    ".recv_any", "local_cluster",
    function(backend)
{
    tryCatch({
        recv_any_local_manager(.con(backend))
    }, error = function(e) {
        stop(.error_worker_comm(e, "'.recv_any,local_cluster-method' failed"))
    })
})

length.local_cluster <-
    function(x)
{
    x$n
}

print.local_cluster <-
    function(x, ...)
{
    cat(
        "class: ", class(x)[1], "\n",
        "worker_id: ", x$worker_id, "\n",
        "timeout: ", x$timeout, " seconds\n",
        "length(): ", length(x), "\n",
        "bpisup(): ", isup_local_manager(.con(x)), "\n",
        sep = ""
    )
}

##
## LocalParam
##

.LocalParam <- setRefClass(
    "LocalParam",
    fields = list(
        backend = "local_cluster", cluster = "character"
    ),
    contains = "BiocParallelParam"
)

.LocalParam_prototype <- c(
    list(backend = NULL, cluster = "auto"),
    .BiocParallelParam_prototype
)

LocalParam <-
    function(workers = snowWorkers(), ..., cluster = c("auto", "snow"))
{
    workers <- as.integer(workers)
    cluster <- match.arg(cluster)
    stopifnot(
        is.integer(workers), length(workers) == 1L, workers > 0L,
        workers < 1000L
    )
    if (.Platform$OS.type == "windows")
        stop("LocalParam() not supported on Windows, use SnowParam()")

    test <- identical(.Platform$OS.type, "windows") ||
        identical(cluster, "snow")
    if (test)
        .worker <- .LocalParam_snowWorker
    else
        .worker <- .LocalParam_multicoreWorker
    backend <- local_cluster(
        n = workers,
        worker = .worker,
        worker_id = "BiocParallel"
    )

    prototype <- .prototype_update(
        .LocalParam_prototype,
        backend = backend,
        workers = workers,
        cluster = cluster,
        ...
    )
    do.call(.LocalParam, prototype)
}

.LocalParam_multicoreWorker <-
    function(path)
{
    mcparallel({
        open(con <- local_worker(path)) # open.connection
        tryCatch(.bpworker_impl(con), error = warning)
    }, detached = TRUE)
}

.LocalParam_snowWorker <-
    function(path)
{
    stop("LocalParam 'snow' cluster not yet supported")
}

setMethod(
    "bpschedule", "LocalParam",
    function(x)
{
    !identical(.Platform$OS.type, "windows")
})

setMethod(
    "bpbackend", "LocalParam",
    function(x)
{
    x$backend
})

setMethod(
    "bpisup", "LocalParam",
    function(x)
{
    isup_local_manager(.con(bpbackend(x)))
})

setMethod(
    "bpstart", "LocalParam",
    function(x, ...)
{
    open_local_cluster(bpbackend(x))
    .bpstart_impl(x)
})

setMethod(
    "bpstop", "LocalParam",
    function(x)
{
    if (!bpisup(x))
        return(invisible(x))

    .bpstop_impl(x)
    close_local_cluster(bpbackend(x))
    invisible(x)
})
