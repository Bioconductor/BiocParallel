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
            msg <- parallel:::recvData(master)
            buffer <<- NULL   ## futile.logger buffer
            success <<- TRUE  ## modified by .try_log()

            if (msg$type == "DONE") {
                closeNode(master)
                break;
            } else if (msg$type == "EXEC") {
                file <- textConnection("sout", "w", local=TRUE)
                sink(file, type="message")
                sink(file, type="output")
                gc(reset=TRUE)
                t1 <- proc.time()
                value <- do.call(msg$data$fun, msg$data$args)
                t2 <- proc.time()
                node <- Sys.info()["nodename"]
                sink(NULL, type="message")
                sink(NULL, type="output")
                close(file)
                value <- list(type = "VALUE", value = value, success = success,
                              time = t2 - t1, tag = msg$data$tag, log = buffer,
                              gc = gc(), node = node, sout = sout)
                parallel:::sendData(master, value)
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
    clusterExport(cl, c(buffer=NULL))

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
    ok <- parallel::clusterApply(cl, seq_along(cl), .bufferload, level=level)
    if (any(sapply(ok, function(j) !is.null(j)))) {
        bpstop(cl)
        stop(conditionMessage(ok[[1]]), 
             "problem loading futile.logger on workers")
    }
}

.bpwriteLog <- function(con, d) {
    .log_internal <- function() {
        message("############### LOG OUTPUT ###############")
        message(sprintf("Task: %i", d$value$tag))
        message(sprintf("Node: %s", d$node))
        message(sprintf("Timestamp: %s", Sys.time()))
        message(sprintf("Success: %s", d$value$success))
        message("Task duration: ")
        print(d$value$time)
        message("Memory use (gc): ")
        print(d$value$gc)
        message("Log messages:")
        message(d$value$log)
        message("stderr and stdout:")
        print(noquote(d$value$sout))
    }
    if (!is.null(con)) {
        sink(con, type = "message")
        sink(con, type = "output")
        .log_internal()    
        sink(NULL, type = "message")
        sink(NULL, type = "output")
        close(con)
    } else .log_internal()
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### bpdynamicClusterApply() and bpdynamicClusterIterate()
###

## These functions are used by all cluster types (SOCK, MPI, FORK) 
## and run on the master. Both enable logging, writing logs/results
## to files and 'stop on error'. The 'cl' argument is an active cluster;
## starting and stopping of 'cl' is done outside these functions.

.clear_cluster <- function(cl, sjobs) 
{
    while (any(sjobs == "running")) {
        d <- parallel:::recvOneData(cl)
        sjobs[d$value$tag] <- "done"
    }
}

## bpdynamicClusterApply():
## derived from snow::dynamicClusterApply.

bpdynamicClusterApply <- function(cl, fun, n, argfun, BPPARAM, progress)
{
    ## result output
    if (!is.na(resdir <- bpresultdir(BPPARAM)))
        BatchJobs:::checkDir(resdir)

    ## setup logging 
    if (bplog(BPPARAM)) {
        if (!is.na(logdir <- bplogdir(BPPARAM)))
            BatchJobs:::checkDir(logdir)
        else 
            con <- NULL
    }

    parallel:::checkCluster(cl)
    p <- length(cl)
    val <- vector("list", n)
    sjobs <- character()
    if (n > 0 && p > 0) {
        ## initial load
        submit <- function(node, job) 
            parallel:::sendCall(cl[[node]], fun, argfun(job), tag = job)
        for (i in 1:min(n, p)) {
            submit(i, i)
            sjobs[i] <- "running"
        }

        for (i in 1:n) {
            ## collect
            d <- parallel:::recvOneData(cl)
            value <- d$value$value
            njob <- d$value$tag
            val[[njob]] <- value
            sjobs[njob] <- "done"
            progress$step()

            ## logging
            if (bplog(BPPARAM)) {
                if (!is.na(logdir)) {
                    lfile <- paste0(logdir, "/", bpjobname(BPPARAM), ".task", 
                                    d$value$tag, ".log")
                    con <- file(lfile, open="w")
                }
                .bpwriteLog(con, d)
            } else cat(paste(d$value$sout, collapse="\n"), "\n")

            ## write results 
            if (!is.na(resdir)) {
                rfile <- paste0(resdir, "/", bpjobname(BPPARAM), ".task", 
                                d$value$tag, ".Rda")
                save(value, file=rfile)
            }

            ## stop on error
            ## let running jobs finish, do not re-load
            if (bpstopOnError(BPPARAM) && !d$value$success) {
                .clear_cluster(cl, sjobs)
                message(paste("error in task ", d$value$tag))
                break
            } else {
                ## re-load 
                j <- i + min(n, p)
                if (j <= n) { 
                    submit(d$node, j)
                    sjobs[j] <- "running"
                }
            }
        }
    }

    ## return results 
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

.reduce <- function(ss, njob, reduce.in.order, success, REDUCE) {
    if (success && !missing(REDUCE)) {
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

    ## setup logging
    if (bplog(BPPARAM)) {
        if (!is.na(logdir <- bplogdir(BPPARAM)))
            BatchJobs:::checkDir(logdir)
        else 
            con <- NULL
    }

    parallel:::checkCluster(cl)
    p <- length(cl)
    ss <- list(sjobs = character(), ## job status ('running', 'done')
               rjobs = list(),      ## non-reduced result
               rindex = 1,          ## reducer index
               val = if (missing(init)) list() else init) ## result

    ## initial load
    inextdata <- NULL
    for (i in 1:p) {
        if (is.null(inextdata <- ITER())) {
            if (i == 1) {
                warning("first invocation of 'ITER()' returned NULL")
                return(list())
            } else {
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

        ## logging
        if (bplog(BPPARAM)) {
            if (!is.na(logdir)) {
                lfile <- paste0(logdir, "/", bpjobname(BPPARAM), ".task", 
                                d$value$tag, ".log")
                con <- file(lfile, open="w")
            }
            .bpwriteLog(con, d)
        } else cat(paste(d$value$sout, collapse="\n"), "\n")

        ## reduce
        ## FIXME: if any worker has an error - can't reduce
        ##        should return ss$rjobs
        success <- d$value$success
        ss <- .reduce(ss, njob, reduce.in.order, success, REDUCE)

        ## write result
        if (!is.na(resdir)) {
            rfile <- paste0(resdir, "/", bpjobname(BPPARAM), ".task", 
                            d$value$tag, ".Rda")
            if (missing(REDUCE))
                save(ss$rjobs, file=rfile)
            else
                save(ss$val, file=rfile)
        }

        ## stop on error
        ## let running jobs finish, do not re-load
        if (bpstopOnError(BPPARAM) && !success) {
            .clear_cluster(cl, ss$sjobs)
            message(paste("error in task ", d$value$tag))
            break
        } else {
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
    }

    ## return results
    if (!is.na(resdir)) {
        NULL 
    } else {
        if (missing(REDUCE))
            ss$rjobs
        else
            list(ss$val)
    }
}
