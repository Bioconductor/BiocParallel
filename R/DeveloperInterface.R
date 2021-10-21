##
## see NAMESPACE section for definitive exports
##

## server

setGeneric(
    ".send_to",
    function(backend, node, value) standardGeneric(".send_to"),
    signature = "backend"
)

setGeneric(
    ".recv_any",
    function(backend) standardGeneric(".recv_any"),
    signature = "backend"
)

setGeneric(
    ".send_all",
    function(backend, value) standardGeneric(".send_all"),
    signature = "backend"
)

setGeneric(
    ".recv_all",
    function(backend) standardGeneric(".recv_all"),
    signature = "backend"
)

## client

setGeneric(
    ".send",
    function(worker, value) standardGeneric(".send"),
    signature = "worker"
)

setGeneric(
    ".recv",
    function(worker) standardGeneric(".recv"),
    signature = "worker"
)

setGeneric(
    ".close",
    function(worker) standardGeneric(".close"),
    signature = "worker"
)

## task manager
setGeneric(
    ".manager",
    function(backend) standardGeneric(".manager"),
    signature = "backend"
)

setGeneric(
    ".manager_send",
    function(manager, value) standardGeneric(".manager_send"),
    signature = "manager"
)

setGeneric(
    ".manager_recv",
    function(manager) standardGeneric(".manager_recv"),
    signature = "manager"
)

setGeneric(
    ".manager_capacity",
    function(manager) standardGeneric(".manager_capacity"),
    signature = "manager"
)

setGeneric(
    ".manager_flush",
    function(manager) standardGeneric(".manager_flush"),
    signature = "manager"
)

setGeneric(
    ".manager_cleanup",
    function(manager) standardGeneric(".manager_cleanup"),
    signature = "manager"
)

## default implementation -- SNOW backend

setMethod(
    ".send_all", "ANY",
    function(backend, value)
{
    for (node in seq_along(backend))
        .send_to(backend, node, value)
})

setMethod(
    ".recv_all", "ANY",
    function(backend)
{
    replicate(length(backend), .recv_any(backend), simplify=FALSE)
})

setMethod(
    ".send_to", "ANY",
    function(backend, node, value)
{
    parallel:::sendData(backend[[node]], value)
    TRUE
})

setMethod(
    ".recv_any", "ANY",
    function(backend)
{
    tryCatch({
        parallel:::recvOneData(backend)
    }, error  = function(e) {
        ## indicate error, but do not stop
        .error_worker_comm(e, "'.recv_any()' data failed")
    })
})

setMethod(
    ".send", "ANY",
    function(worker, value)
{
    parallel:::sendData(worker, value)
})

setMethod(
    ".recv", "ANY",
    function(worker)
{
    tryCatch({
        parallel:::recvData(worker)
    }, error = function(e) {
        ## indicate error, but do not stop
        .error_worker_comm(e, "'.recv()' data failed")
    })
})

setMethod(
    ".close", "ANY",
    function(worker)
{
    parallel:::closeNode(worker)
})


setMethod(
    ".close", "ANY",
    function(worker)
{
    parallel:::closeNode(worker)
})

## default task manager implementation
setMethod(
    ".manager", "ANY",
    function(backend)
{
    manager <- new.env(parent = emptyenv())
    manager$backend <- backend
    manager$availableWorker <- new.env(parent = emptyenv())
    lapply(seq_along(manager$backend),
           function(x) manager$availableWorker[[as.character(x)]] <- x)
    manager
})

setMethod(
    ".manager_send", "ANY",
    function(manager, value)
{
    workerIdx <- names(manager$availableWorker)[1]
    rm(list = workerIdx, envir = manager$availableWorker)
    .send_to(manager$backend, as.integer(workerIdx), value)
})

setMethod(
    ".manager_recv", "ANY",
    function(manager)
{
    result <- .recv_any(manager$backend)
    manager$availableWorker[[as.character(result$node)]] <- result$node
    list(result)
})

setMethod(
    ".manager_capacity", "ANY",
    function(manager)
{
    length(manager$backend)
})

setMethod(
    ".manager_flush", "ANY",
    function(manager) manager
)

setMethod(
    ".manager_cleanup", "ANY",
    function(manager) manager
)
