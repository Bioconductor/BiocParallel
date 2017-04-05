

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### snow::MPI
###

bprunMPIslave <- function() {
    comm <- 1
    intercomm <- 2
    Rmpi::mpi.comm.get.parent(intercomm)
    Rmpi::mpi.intercomm.merge(intercomm,1,comm)
    Rmpi::mpi.comm.set.errhandler(comm)
    Rmpi::mpi.comm.disconnect(intercomm)

    bploop(snow::makeMPImaster(comm))

    Rmpi::mpi.comm.disconnect(comm)
    Rmpi::mpi.quit()
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### parallel::FORK
###

.bpfork <- function (nnodes, timeout, host, port)
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
        .bpforkChild(host, port, rank, timeout)
        cl[[rank]] <- .bpforkConnect(host, port, rank, timeout)
    }

    class(cl) <- c("SOCKcluster", "cluster")
    cl
}

.bpforkChild <-
    function(host, port, rank, timeout)
{
    parallel::mcparallel({
        con <- NULL
        suppressWarnings({
            while (is.null(con)) {
                con <- tryCatch({
                    socketConnection(host, port, FALSE, TRUE, "a+b",
                                     timeout = timeout)
                }, error=function(e) {})
            }
        })
        node <- structure(list(con = con), class = "SOCK0node")
        bploop(node)
    }, detached=TRUE)
}

.bpforkConnect <-
    function(host, port, rank, timeout)
{
    con <- socketConnection(host, port, TRUE, TRUE, "a+b", timeout = timeout)
    structure(list(con = con, host = host, rank = rank),
              class = c("forknode", "SOCK0node"))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### logs and results
###

.initiateLogging <- function(BPPARAM) {
    cl <- bpbackend(BPPARAM)

    .bufferload <- function(i, log, threshold) {
        tryCatch({
            .log_load(log, threshold)
            .log_appender()
        }, error = identity)
    }
    ok <- tryCatch({
        parallel::clusterApply(cl, seq_along(cl), .bufferload,
                               log=bplog(BPPARAM),
                               threshold=bpthreshold(BPPARAM))
    }, error=function(e) {
        stop(.error_worker_comm(e, "'.initiateLogging()' failed"))
    })
    if (!all(vapply(ok, is.null, logical(1)))) {
        bpstop(cl)
        stop(conditionMessage(ok[[1]]), 
             "problem loading futile.logger on workers")
    }
}

.bpwriteLog <- function(con, d) {
    .log_internal <- function() {
        message(
            "############### LOG OUTPUT ###############\n",
            "Task: ", d$value$tag,
            "\nNode: ", d$node,
            "\nTimestamp: ", Sys.time(),
            "\nSuccess: ", d$value$success,
            "\nTask duration:\n",
            paste(capture.output(d$value$time), collapse="\n"),
            "\nMemory used:\n", paste(capture.output(gc()), collapse="\n"),
            "\nLog messages:",
            paste(d$value$log, collapse="\n"),
            "\nstderr and stdout:\n",
            paste(noquote(d$value$sout), collapse="\n")
        )
    }
    if (!is.null(con)) {
        sink(con, type = "message")
        sink(con, type = "output")
        .log_internal()    
        sink(NULL, type = "message")
        sink(NULL, type = "output")
    } else .log_internal()
}
