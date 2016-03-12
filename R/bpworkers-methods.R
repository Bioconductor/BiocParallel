setMethod("bpworkers", "missing",
    function(x)
{
    x <- registered()[[1]]
    bpworkers(x)
})

bpnworkers <- function(x) {
    n <- bpworkers(x)
    if (!is.numeric(n))
        n <- length(n)
    n
}
