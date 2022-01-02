setMethod("bpstop", "ANY", function(x) invisible(x))

setMethod("bpstop", "missing",
    function(x)
{
    x <- registered()[[1]]
    bpstop(x)
})

##
## .bpstop_impl: common functionality after bpisup() is no longer TRUE
##

.bpstop_nodes <-
    function(x)
{
    manager <- .manager(x)
    .manager_send_all(manager, .DONE())
    TRUE
}

.bpstop_impl <-
    function(x)
{
    bpisup(x) && .bpstop_nodes(x)
    .ClusterManager$drop(x$.uid)
    invisible(x)
}
