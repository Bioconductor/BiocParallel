## .BiocParallelParam_prototype, .prototype_update, .prettyPath,
## .bpstart_impl, .bpstop_impl, .bplapply_impl, .bpiterate_impl,
## .error_worker_comm

## server
setGeneric(
    ".send_to",
    function(cluster, node, value) standardGeneric(".send_to"),
    signature = "cluster"
)

setGeneric(
    ".recv_any",
    function(cluster) standardGeneric(".recv_any"),
    signature = "cluster"
)

setGeneric(
    ".send_all",
    function(cluster, value) standardGeneric(".send_all"),
    signature = "cluster"
)

setGeneric(
    ".recv_all",
    function(cluster) standardGeneric(".recv_all"),
    signature = "cluster"
)

## client

setGeneric(
    ".send",
    function(cluster, value) standardGeneric(".send"),
    signature = "cluster"
)

setGeneric(
    ".recv",
    function(cluster) standardGeneric(".recv"),
    signature = "cluster"
)

setGeneric(
    ".close",
    function(cluster) standardGeneric(".close"),
    signature = "cluster"
)

## default implementation -- SNOW cluster

setMethod(
    ".send_all", "ANY",
    function(cluster, value)
{
    for (node in seq_along(cluster))
        .send_to(cluster, node, value)
})

setMethod(
    ".recv_all", "ANY",
    function(cluster)
{
    replicate(length(cluster), .recv_any(cluster), simplify=FALSE)
})

setMethod(
    ".send_to", "ANY",
    function(cluster, node, value)
{
    parallel:::sendData(cluster[[node]], value)
    TRUE
})

setMethod(
    ".recv_any", "ANY",
    function(cluster)
{
    tryCatch({
        parallel:::recvOneData(cluster)
    }, error  = function(e) {
        ## indicate error, but do not stop
        .error_worker_comm(e, "'.recv_any()' data failed")
    })
})

setMethod(
    ".send", "ANY",
    function(cluster, value)
{
    parallel:::sendData(cluster, value)
})

setMethod(
    ".recv", "ANY",
    function(cluster)
{
    tryCatch({
        parallel:::recvData(cluster)
    }, error = function(e) {
        ## indicate error, but do not stop
        .error_worker_comm(e, "'.recv()' data failed")
    })
})

setMethod(
    ".close", "ANY",
    function(cluster)
{
    parallel:::closeNode(cluster)
})
