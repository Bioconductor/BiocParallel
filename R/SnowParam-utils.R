

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### snow::MPI
###

bprunMPIslave <- function() {
    .Deprecated("bprunMPIworker")
    bprunMPIworker()
}

bprunMPIworker <- function() {
    comm <- 1
    intercomm <- 2
    Rmpi::mpi.comm.get.parent(intercomm)
    Rmpi::mpi.intercomm.merge(intercomm,1,comm)
    Rmpi::mpi.comm.set.errhandler(comm)
    Rmpi::mpi.comm.disconnect(intercomm)

    .bpworker_impl(snow::makeMPImaster(comm))

    Rmpi::mpi.comm.disconnect(comm)
    Rmpi::mpi.quit()
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### parallel::FORK
###

.bpfork <- function (nnodes, host, port, socket_timeout)
{
    nnodes <- as.integer(nnodes)
    if (is.na(nnodes) || nnodes < 1L)
        stop("'nnodes' must be >= 1")

    if (length(host) != 1L || is.na(host) || !is.character(host))
        stop("'host' must be character(1)")
    if (length(port) != 1L || is.na(port) || !is.integer(port))
        stop("'port' must be integer(1)")

    cl <- vector("list", nnodes)
    for (rank in seq_along(cl)) {
        .bpforkChild(host, port, rank, socket_timeout)
        cl[[rank]] <- .bpforkConnect(host, port, rank, socket_timeout)
    }

    class(cl) <- c("SOCKcluster", "cluster")
    cl
}

.bpforkChild <-
    function(host, port, rank, socket_timeout)
{
    parallel::mcparallel({
        con <- NULL
        suppressWarnings({
            while (is.null(con)) {
                con <- tryCatch({
                    socketConnection(
                        host, port, FALSE, TRUE, "a+b",
                        timeout = socket_timeout
                    )
                }, error=function(e) {})
            }
        })
        node <- structure(list(con = con), class = "SOCK0node")
        .bpworker_impl(node)
    }, detached=TRUE)
}

.bpforkConnect <-
    function(host, port, rank, socket_timeout)
{
    con <- socketConnection(
        host, port, TRUE, TRUE, "a+b", timeout = socket_timeout
    )
    structure(list(con = con, host = host, rank = rank),
              class = c("forknode", "SOCK0node"))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### EXEC command cache
###

## read/write the static value
.load_task_static <-
    function(value)
{
    static_data <- .task_const(value)
    if (is.null(static_data)) {
        static_data <- options("BIOCPARALLEL_SNOW_STATIC")[[1]]
        .task_remake(value, static_data)
    } else {
        options(BIOCPARALLEL_SNOW_STATIC = static_data)
        value
    }
}

.clean_task_static <-
    function()
{
    options(BIOCPARALLEL_SNOW_STATIC = NULL)
}
