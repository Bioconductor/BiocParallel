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
            success <<- TRUE ## modified in .try() and .try_log() 

            if (msg$type == "DONE") {
                closeNode(master)
                break;
            } else if (msg$type == "EXEC") {
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

    .bufferload <- function(i, level) {
        tryCatch({
            attachNamespace("futile.logger")
            flog.threshold(level)
            fun <- function(line)
                buffer <<- c(buffer, line)
            flog.appender(fun, 'ROOT')
        }, error = function(e) e
        )
    }
    ok <- clusterApply(cl, seq_along(cl), .bufferload, level=level)
    if (any(sapply(ok, function(j) !is.null(j))))
        stop("problem loading futile.logger on workers")
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
        message(sprintf("Task: %i", jobid))
        message(sprintf("Node: %s", node))
        message(sprintf("Timestamp: %s", Sys.time()))
        message(sprintf("Success: %s", value$success))
        message("Task duration: ")
        print(value$time)
        message("Memory use (gc): ")
        print(value$gc)
        message("Log messages:")
        message(value$log)
        message("stderr:")
        message("stdout:")
    }
}

.bplogSetUp <- function(logdir) {
    BatchJobs:::checkDir(logdir)
    file(paste0(logdir, "/BPLOG.out"), open="w")
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### bpdynamicClusterApply() and bpdynamicClusterIterate()
###

## These functions are used by all cluster types (SOCK, MPI, FORK) 
## and run on the master. Both enable logging, writing logs/results
## to files and 'stop on error'.

## bpdynamicClusterApply():
## derived from snow::dynamicClusterApply.

bpdynamicClusterApply <- function(cl, fun, n, argfun, BPPARAM, progress)
{
    ## result output
    if (!is.na(resdir <- bpresultdir(BPPARAM)))
        BatchJobs:::checkDir(resdir)

    ## log connection
    if (bplog(BPPARAM) && !is.na(bplogdir(BPPARAM))) {
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
        ## initial load
        submit <- function(node, job) 
            parallel:::sendCall(cl[[node]], fun, argfun(job), tag = job)
        for (i in 1:min(n, p)) 
            submit(i, i)

        ## collect and re-load
        for (i in 1:n) {
            d <- parallel:::recvOneData(cl)
            value <- d$value$value
            val[[d$value$tag]] <- value
            progress$step()
            ## logging
            if (bplog(BPPARAM))
                .bpwriteLog(con, d)
            ## stop on error
            if (bpstopOnError(BPPARAM) && !d$value$success) {
                warning(paste0("error in task ", d$value$tag))
                bpstop(cl)
                return(val)
            }
            j <- i + min(n, p)
            if (j <= n) 
                submit(d$node, j)
            if (!is.na(resdir))
                save(value, file=paste0(resdir, "/TASK", d$value$tag, ".Rda"))
        }
    }

    if (!is.na(resdir))
        NULL 
    else val 
}

## bpdynamicClusterIterate():
## Derived from snow::dynamicClusterApply and parallel::mclapply.
## - length of 'X' is unknown (defined by ITER())
## - results not pre-allocated; list grows each iteration if no REDUCE
## - args wrapped in arglist with different chunks from ITER()

.reduce_while_done <- function(ss, REDUCE) {
    while (ss$sjobs[ss$rindex] == "done") {
        ss$val <- REDUCE(ss$val, ss$rjobs[[ss$rindex]])
        ss$rjobs[[ss$rindex]] <- NA 
        if (ss$rindex == length(ss$sjobs))
            break
        ss$rindex <- ss$rindex + 1
    }
    ss
}

.reduce <- function(ss, njob, REDUCE, reduce.in.order) {
    if (reduce.in.order) {
        if (njob == 1) {
            if (length(ss$val))
                ss$val <- REDUCE(ss$val, ss$rjobs[[njob]])
            else
                ss$val <- ss$rjobs[[njob]]
            ss$rjobs[[njob]] <- NA
            ss$rindex <- ss$rindex + 1 
            ss <- .reduce_while_done(ss, REDUCE)
        } else if (njob == ss$rindex) { 
            ss <- .reduce_while_done(ss, REDUCE)
        }
    } else {
        if (length(ss$val))
            ss$val <- REDUCE(ss$val, unlist(ss$rjobs[[njob]]))
        else
           ss$val <- unlist(ss$rjobs[[njob]])
    }
    ss 
}

bpdynamicClusterIterate <- function(cl, fun, ITER, REDUCE, init, 
                                    reduce.in.order=FALSE, BPPARAM, ...)
{
    if (missing(REDUCE) && reduce.in.order)
        stop("REDUCE must be provided when 'reduce.in.order = TRUE'")
    if (missing(REDUCE) && !missing(init))
        stop("REDUCE must be provided when 'init' is given")

    ## result output
    if (!is.na(resdir <- bpresultdir(BPPARAM)))
        BatchJobs:::checkDir(resdir)

    ## log connection
    if (bplog(BPPARAM) && !is.na(bplogdir(BPPARAM))) {
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
    ss <- list(sjobs = character(), ## job status ('running', 'done')
               rjobs = list(),      ## non-reduced result
               rindex = 1,          ## reducer index
               val = if (missing(init)) list() else init) ## result

    inextdata <- NULL
    ## initial load
    for (i in 1:p) {
        if (is.null(inextdata <- ITER())) {
            if (i == 1) {
                warning("first invocation of 'ITER()' returned NULL")
                return(list())
            } else {
                bpstop(cl)
                break
            }
        } else {
            ss$sjobs[i] <- "running"
            arglist <- c(list(inextdata), list(...))
            parallel:::sendCall(cl[[i]], fun, arglist, tag = i)
        }
    }
 
    repeat {
        ## collect
        d <- parallel:::recvOneData(cl)
        njob <- d$value$tag
        ss$rjobs[[njob]] <- d$value$value 
        ss$sjobs[njob] <- "done"

        ## stop on error
        if (bpstopOnError(BPPARAM) && !d$value$success) {
            warning(paste0("error in task ", njob))
            bpstop(cl)
            return(ss$val)
        }

        ## reduce 
        if (!missing(REDUCE))
            ss <- .reduce(ss, njob, REDUCE, reduce.in.order)

        ## log
        if (bplog(BPPARAM))
            .bpwriteLog(con, ss$val)

        ## re-load
        if (!is.null(inextdata <- ITER())) {
            i <- i + 1
            ss$sjobs[i] <- "running"
            arglist <- c(list(inextdata), list(...))
            parallel:::sendCall(cl[[d$node]], fun, arglist, tag = i)
        } else { 
            if ((length(ss$sjobs) == 0) || (all(ss$sjobs == "done")))
                break
        }
    }

    ## results
    if (!is.na(resdir)) {
        NULL 
    } else {
        if (missing(REDUCE))
            ss$rjobs
        else
            list(ss$val)
    }
}
