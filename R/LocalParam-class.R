##
## local_client
##

setOldClass("local_client")

local_client <-
    function(path, timeout = 30L)
{
    mode <- "w+b"
    .Call(.local_client, path, mode, timeout);
}

setMethod(
    ".recv", "local_client",
    function(worker)
{
    res <- tryCatch({
        unserialize(worker)
    }, error = function(e) {
        .error_worker_comm(e, "'.recv,local_client-method' failed")
    })
})

setMethod(
    ".send", "local_client",
    function(worker, value)
{
    serialize(value, worker, xdr = FALSE)
})

setMethod(
    ".close", "local_client",
    function(worker)
{
    close(worker)                       # close.connection
})

##
## local_server
##

setOldClass("local_server")

local_server <-
    function(path, timeout = 30L, backlog = 5L)
{
    mode <- "w+b"
    .Call(.local_server, path, mode, timeout, backlog)
}

local_server_accept <-
    function(srv)
{
    .Call(.local_server_accept, srv)
}

local_server_selectfd <-
    function(srv, mode = c("r", "w"))
{
    mode <- match.arg(mode)
    .Call(.local_server_selectfd, srv, mode)
}

local_server_activefds <-
    function(srv)
{
    .Call(.local_server_activefds, srv);
}

local_server_set_activefd <-
    function(srv, fd)
{
    .Call(.local_server_set_activefd, srv, fd)
}

send_to_local_server <-
    function(x, node, value)
{
    fd <- local_server_activefds(x)[node]
    local_server_set_activefd(x, fd)
    serialize(value, x, xdr = FALSE)
}


recv_any_local_server <-
    function(x)
{
    fd <- local_server_selectfd(x)
    length(fd) || stop("'recv_any()' timeout")

    fd <- fd[sample.int(length(fd), 1L)]

    local_server_set_activefd(x, fd)
    value <- unserialize(x)

    node <- match(fd, local_server_activefds(x))
    structure(list(value = value, node = node), class = "recv_any")
}

isup_local_server <-
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
             client, client_id)
{
    fields <- list(
        con = NULL, n = n, timeout = timeout,
        client = client, client_id = client_id
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
    stopifnot(!isup_local_server(.con(x)))

    path <- tempfile(fileext = ".skt")
    n <- x$n
    timeout <- x$timeout

    x$con <- local_server(path, timeout = timeout, backlog = min(n, 128L))
    open(.con(x), "w+b")                # open.connection

    while (n > 0L) {
        n0 <- min(n, 128L)              # maximum backlog 128
        n <- n - n0
        replicate(n0, x$client(path), simplify = FALSE)
        replicate(n0, local_server_accept(.con(x)), simplify = FALSE)
    }

    invisible(x)
}

close_local_cluster <-
    function(x)
{
    if (isup_local_server(.con(x))) {
        close(.con(x))                  # close.connection
        .con(x) <- NULL
    }
    invisible(x)
}

setMethod(
    ".send_to", "local_cluster",
    function(backend, node, value)
{
    send_to_local_server(.con(backend), node, value)
    TRUE
})

setMethod(
    ".recv_any", "local_cluster",
    function(backend)
{
    tryCatch({
        recv_any_local_server(.con(backend))
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
        "client_id: ", x$client_id, "\n",
        "timeout: ", x$timeout, " seconds\n",
        "length(): ", length(x), "\n",
        "bpisup(): ", isup_local_server(.con(x)), "\n",
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
        .client <- .LocalParam_snowClient
    else
        .client <- .LocalParam_multicoreClient
    backend <- local_cluster(
        n = workers,
        client = .client,
        client_id = "BiocParallel"
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

.LocalParam_multicoreClient <-
    function(path)
{
    mcparallel({
        open(con <- local_client(path)) # open.connection
        tryCatch(.bpworker_impl(con), error = warning)
    }, detached = TRUE)
}

.LocalParam_snowClient <-
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
    isup_local_server(.con(bpbackend(x)))
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
