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
