.TransientMulticoreParam <-
    setRefClass(
        "TransientMulticoreParam",
        contains = "MulticoreParam"
    )

TransientMulticoreParam <-
    function(param)
{
    param <- as(param, "TransientMulticoreParam")
    bpstart(param)
}

.TRANSIENTMULTICOREPARAM_JOBNODE <- new.env(parent=emptyenv())
.TRANSIENTMULTICOREPARAM_RESULT <- new.env(parent=emptyenv())

setMethod(
    "bpstart", "TransientMulticoreParam",
    function(x, ...)
{
    parallel::mccollect(wait=TRUE)

    rm(
        list=ls(envir = .TRANSIENTMULTICOREPARAM_JOBNODE),
        envir = .TRANSIENTMULTICOREPARAM_JOBNODE
    )
    rm(
        list = ls(envir = .TRANSIENTMULTICOREPARAM_RESULT),
        envir = .TRANSIENTMULTICOREPARAM_RESULT
    )
    .bpstart_impl(x)
})

setMethod(
    "bpstop", "TransientMulticoreParam",
    function(x)
{
    .bpstop_impl(x)
})

setMethod(
    "bpbackend", "TransientMulticoreParam",
    function(x)
{
    x
})

setMethod(
    "length", "TransientMulticoreParam",
    function(x)
{
    bpnworkers(x)
})

##
## send / recv
##

setMethod(
    ".recv_all", "TransientMulticoreParam",
    function(backend)
{
    replicate(length(backend), .recv_any(backend), simplify=FALSE)
})

setMethod(
    ".send_to", "TransientMulticoreParam",
    function(backend, node, value)
{
    if (value$type == "EXEC") {
        job <- parallel::mcparallel(.bpworker_EXEC(value))
        id <- as.character(job$pid)
        .TRANSIENTMULTICOREPARAM_JOBNODE[[id]] <- node
    }
    TRUE
})

setMethod(
    ".recv_any", "TransientMulticoreParam",
    function(backend)
{
    .BUFF <- .TRANSIENTMULTICOREPARAM_RESULT # alias
    tryCatch({
        while (!length(.BUFF)) {
            result <- parallel::mccollect(wait = FALSE, timeout = 1)
            for (id in names(result))
                .BUFF[[id]] <- result[[id]]
        }
        id <- head(names(.BUFF), 1L)

        value <- .BUFF[[id]]
        rm(list = id, envir = .BUFF)
        node <- .TRANSIENTMULTICOREPARAM_JOBNODE[[id]]
        rm(list = id, envir = .TRANSIENTMULTICOREPARAM_JOBNODE)
        list(node = node, value = value)
    }, error  = function(e) {
        ## indicate error, but do not stop
        .error_worker_comm(e, "'.recv_any()' data failed")
    })
})

setMethod(
    ".send", "TransientMulticoreParam",
    function(worker, value)
{
    stop("'.send,TransientMulticoreParam-method' not implemented")
})

setMethod(
    ".recv", "TransientMulticoreParam",
    function(worker)
{
    stop("'.recv,TransientMulticoreParam-method' not implemented")
})

setMethod(
    ".close", "TransientMulticoreParam",
    function(worker)
{
    stop("'.close,TransientMulticoreParam-method' not implemented")
})

setMethod(".manager", "TransientMulticoreParam", .manager_ANY)
