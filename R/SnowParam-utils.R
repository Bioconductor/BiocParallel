### =========================================================================
### Low-level cluster utilities
### -------------------------------------------------------------------------

### Support for SOCK, MPI and FORK connections.
### Derived from snow version 0.3-13 by Luke Tierney 
### Derived from parallel version 2.16.0 by R Core Team

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Worker loop used by SOCK, MPI and FORK when log=TRUE.
### Error handling is done in .composeTry_log.

bpslaveLoop <- function(master)
{
    repeat
        tryCatch({
            msg <- recvData(master)
            buffer <<- NULL  ## futile.logger buffer

            if (msg$type == "DONE") {
                closeNode(master)
                break;
            } else if (msg$type == "EXEC") {
                success <- TRUE
                gc(reset=TRUE)
                t1 <- proc.time()
                value <- do.call(msg$data$fun, msg$data$args)
                t2 <- proc.time()
                node <- Sys.info()["nodename"]
                value <- list(type = "VALUE", value = value, success = success,
                              time = t2 - t1, tag = msg$data$tag, log = buffer,
                              gc = gc(), node = node)
                sendData(master, value)
            }
        }, interrupt = function(e) NULL)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### snow::MPI
###

bprunMPIslave <- function() {
    comm <- 1
    intercomm <- 2
    mpi.comm.get.parent(intercomm)
    mpi.intercomm.merge(intercomm,1,comm)
    mpi.comm.set.errhandler(comm)
    mpi.comm.disconnect(intercomm)

    bpslaveLoop(makeMPImaster(comm))

    mpi.comm.disconnect(comm)
    mpi.quit()
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### parallel::FORK
###

.bpmakeForkCluster <- function (nnodes = getOption("mc.cores", 2L), ...) 
{
    nnodes <- as.integer(nnodes)
    if (is.na(nnodes) || nnodes < 1L) 
        stop("'nnodes' must be >= 1")
    parallel:::.check_ncores(nnodes)
    cl <- vector("list", nnodes)
    for (i in seq_along(cl)) cl[[i]] <- .bpnewForkNode(..., rank = i)
    class(cl) <- c("SOCKcluster", "cluster")
    cl
}

.bpnewForkNode <- function(..., 
                           options = parallel:::defaultClusterOptions, rank)
{
    getClusterOption <- parallel:::getClusterOption
    options <- parallel:::addClusterOptions(options, list(...))
    outfile <- getClusterOption("outfile", options)
    port <- getClusterOption("port", options)
    timeout <- getClusterOption("timeout", options)
    renice <- getClusterOption("renice", options)

    environment(bpslaveLoop) <- getNamespace('parallel')
    f <- mcfork()
    if (inherits(f, "masterProcess")) { # the slave
        on.exit(mcexit(1L, structure("fatal error in wrapper code",
                                  class = "try-error")))
        # closeStdout()
        master <- "localhost"
        makeSOCKmaster <- function(master, port, timeout)
        {
            port <- as.integer(port)
            ## maybe use `try' and sleep/retry if first time fails?
            con <- socketConnection(master, port = port, blocking = TRUE,
                                    open = "a+b", timeout = timeout)
            structure(list(con = con), class = "SOCK0node")
        }
        parallel:::sinkWorkerOutput(outfile)
        msg <- sprintf("starting worker pid=%d on %s at %s\n",
                       Sys.getpid(), paste(master, port, sep = ":"),
                       format(Sys.time(), "%H:%M:%OS3"))
        cat(msg)
        ## allow this to quit when the loop is done.
        tools::pskill(Sys.getpid(), tools::SIGUSR1)
        if(!is.na(renice) && renice) ## ignore 0
            tools::psnice(Sys.getpid(), renice)

        bpslaveLoop(makeSOCKmaster(master, port, timeout))
        mcexit(0L)
    }

    con <- socketConnection("localhost", port = port, server = TRUE,
                            blocking = TRUE, open = "a+b", timeout = timeout)
    structure(list(con = con, host = "localhost", rank = rank),
              class = c("forknode", "SOCK0node"))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### logs and results
###

.initiateLogging <- function(BPPARAM) {
    level <- bpthreshold(BPPARAM)
    flog.threshold(level)
    flog.info("loading futile.logger on workers")
    cl <- bpbackend(BPPARAM)
    clusterExport(cl, c(buffer=NULL))  ## global assignment
    parallel::clusterApply(cl, seq_along(cl), 
        function(i, level) {
            attachNamespace("futile.logger")
            flog.threshold(level)
            fun <- function(line) 
                buffer <<- c(buffer, gsub("\n", "", line))
            flog.appender(fun, 'ROOT')
        }, level=level)
}

.bpwriteLog <- function(con, d) {
    node <- d$node
    value <- d$value
    jobid <- d$value$tag
    if (is.null(con)) {
        if (!is.null(value$log))
            print(value$log)
    } else {
        sink(con, type = "message")
        sink(con, type = "output")
        message("############### LOG OUTPUT ###############")
        message(sprintf("Job: %i", jobid))
        message(sprintf("Node: %s", node))
        message(sprintf("Timestamp: %s", Sys.time()))
        message(sprintf("Success: %s", value$success))
        message("Job duration: ")
        print(value$time)
        message("Memory use (gc): ")
        print(value$gc)
        message("Log messages:")
        message(value$log)
    }
}

.bplogSetUp <- function(logdir) {
    BatchJobs:::checkDir(logdir)
    file(paste0(logdir, "/LOGFILE.out"), open="w")
}


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### bpdynamicClusterApply() and bpdynamicClusterIterate()
###

## These functions are used by all cluster types (SOCK, MPI, FORK) 
## and run on the master. Both enable logging, writing logs/results
## to files and 'stop on error'.

## bpdynamicClusterApply:
## Derived from snow::dynamicClusterApply.
## - length of 'X' is known
## - results list is pre-allocated
## - args sent to workers as argfun(), invoked with index 'i'
bpdynamicClusterApply <- function(cl, fun, n, argfun, BPPARAM)
{
    ## result output
    if (length(resdir <- bpresultdir(BPPARAM)))
        BatchJobs:::checkDir(resdir)

    ## log connection
    if (bplog(BPPARAM) && length(bplogdir(BPPARAM))) {
        con <- .bplogSetUp(bplogdir(BPPARAM))
        on.exit({ 
            sink(NULL, type = "message")
            sink(NULL, type = "output")
            close(con)
        })
    } else con <- NULL

    snow::checkCluster(cl)
    p <- length(cl)
    val <- vector("list", n)
    if (n > 0 && p > 0) {
        submit <- function(node, job) 
            parallel:::sendCall(cl[[node]], fun, argfun(job), tag = job)
        for (i in 1:min(n, p)) 
            submit(i, i)
        for (i in 1:n) {
            d <- parallel:::recvOneData(cl)
            value <- d$value$value
            val[[d$value$tag]] <- value
            ## logging
            if (bplog(BPPARAM))
                .bpwriteLog(con, d)
            ## stop on error
            if (bpstopOnError(BPPARAM) && !d$value$success) {
                warning(paste0("error in job ", d$value$tag))
                return(val)
            }
            j <- i + min(n, p)
            if (j <= n) 
                submit(d$node, j)
            if (length(resdir))
                save(value, file=paste0(resdir, "/JOB", d$value$tag, ".Rda"))
        }
    }

    if (length(bpresultdir(BPPARAM)))
        NULL 
    else val 
}

## bpdynamicClusterIterate():
## Derived from snow::dynamicClusterApply and parallel::mclapply.
## - length of 'X' is unknown (defined by ITER())
## - results not pre-allocated; list grows each iteration if REDUCE is provided
## - args wrapped in arglist with different chunks from ITER()
bpdynamicClusterIterate <- function(cl, fun, ITER, REDUCE, init, 
                                    reduce.in.order=FALSE, BPPARAM, ...)
{
    if (missing(REDUCE) && reduce.in.order)
        stop("REDUCE must be provided when 'reduce.in.order = TRUE'")

    ## result output
    if (length(resdir <- bpresultdir(BPPARAM)))
        BatchJobs:::checkDir(resdir)

    ## log connection
    if (bplog(BPPARAM) && length(bplogdir(BPPARAM))) {
        con <- .bplogSetUp(bplogdir(BPPARAM))
        on.exit({ 
            sink(NULL, type = "message")
            sink(NULL, type = "output")
            close(con)
        })
    } else con <- NULL

    snow::checkCluster(cl)
    p <- length(cl)

    ## initialize
    sjobs <- character()                    ## job status ('running', 'done')
    rjobs <- list()                         ## non-reduced result
    rindex <- 1                             ## reducer index
    val <- list()                           ## final result
    if (!missing(REDUCE) & !missing(init))
        val <- init

    inextdata <- NULL
    ## load available workers
    for (i in 1:p) {
        if (is.null(inextdata <- ITER())) {
            warning("first invocation of 'ITER()' returned NULL")
            return(list())
        } else {
            sjobs[i] <- "running"
            arglist <- c(list(inextdata), list(...))
            parallel:::sendCall(cl[[i]], fun, arglist, tag = i)
        }
    }
 
    repeat {
        ## collect
        d <- parallel:::recvOneData(cl)
        njob <- d$value$tag
        rjobs[[njob]] <- d$value$value 
        sjobs[njob] <- "done"
        ## stop on error
        if (bpstopOnError(BPPARAM) && !d$value$success) {
            warning(paste0("error in job ", njob))
            return(val)
        }

        ## REDUCE
        if (!missing(REDUCE))  {
            if (reduce.in.order) {
                if (njob == 1) {
                    if (length(val))
                        val <- REDUCE(val, rjobs[[njob]])
                    else
                        val <- rjobs[[njob]]
                    rjobs[[njob]] <- NA
                    rindex <- rindex + 1 
                    while (sjobs[rindex] == "done") {
                        val <- REDUCE(val, rjobs[[rindex]])
                        rjobs[[rindex]] <- NA 
                        if (rindex == length(sjobs))
                            break
                        else
                            rindex <- rindex + 1
                    }
                } else if (njob == rindex) { 
                    while (sjobs[rindex] == "done") {
                        val <- REDUCE(val, rjobs[[rindex]])
                        rjobs[[rindex]] <- NA
                        if (rindex == length(sjobs))
                            break
                        else
                            rindex <- rindex + 1
                    }
                }
            } else {
                if (length(val))
                    val <- REDUCE(val, unlist(rjobs[[njob]]))
                else
                   val <- unlist(rjobs[[njob]])
            }
        }

        ## logging
        if (bplog(BPPARAM))
            .bpwriteLog(con, val)

        ## load remaining jobs
        if (!is.null(inextdata <- ITER())) {
            i <- i + 1
            sjobs[i] <- "running"
            arglist <- c(list(inextdata), list(...))
            parallel:::sendCall(cl[[d$node]], fun, arglist, tag = i)
        } else { 
            if ((length(sjobs) == 0) || (all(sjobs == "done")))
                break
        }
    }

    ## results
    if (length(bpresultdir(BPPARAM))) {
        NULL 
    } else {
        if (missing(REDUCE))
            rjobs
        else
            list(val)
    }
}
