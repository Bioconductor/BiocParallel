## This multi-core implementation of bpiterate() is a modified
## version of sclapply() by Gregoire Pau.

.bpiterate_multicore <- function(ITER, FUN, ..., REDUCE, init,
    reduce.in.order = FALSE, mc.set.seed = TRUE, mc.silent = FALSE, 
    mc.cores = getOption("mc.cores", 2L),
    mc.cleanup = TRUE)
{
    if (!missing(init) && !reduce.in.order) {
        warning("'reduce.in.order' is set to TRUE when 'init' is provided")
        reduce.in.order <- TRUE
    }

    ## initialize
    sjobs <- character()                    ## job state
    rjobs <- list()                         ## job result
    pnodes <- vector(mode="list", mc.cores) ## node process
    jnodes <- rep(NA, mc.cores)             ## node job id
    rindex <- 1                             ## reducer index
    res <- list()
    if (!missing(REDUCE) & !missing(init))
        res <- init

    ## cleanup based on mclapply
    on.exit(.cleanup(pnodes[!sapply(pnodes, is.null)], mc.cleanup))

    i <- 0; inextdata <- NULL
    repeat {
        ## new job to process?
        if (is.null(inextdata))
            inextdata <- ITER()
        ## all jobs done?
        if (is.null(inextdata))
            if ((length(sjobs) == 0) || (all(sjobs == "done")))
                break

        ## fire FUN(inextdata) on node i
        repeat {
            fire <- TRUE
            i <- (i %% length(pnodes)) + 1L
            process <- pnodes[[i]]
            if (!is.null(process)) {
                status <- mccollect(process, wait=FALSE)
                if (is.null(status)) {
                    ## node busy
                    fire <- FALSE
                } else {
                    ## node done
                    mccollect(process)      ## kill; is this needed?
                    jindex <- jnodes[i]
                    rjobs[[jindex]] <- status[[1L]]
                    sjobs[jindex] <- "done"

                    ## reduce.in.order = TRUE
                    if (!missing(REDUCE) && reduce.in.order) {
                        if (jindex == 1) {
                            if (!missing(init))
                                res <- REDUCE(init, rjobs[[jindex]])
                            else
                                res <- rjobs[[jindex]]
                            rjobs[[jindex]] <- NA
                            rindex <- rindex + 1 
                            while (sjobs[rindex] == "done") {
                                res <- REDUCE(res, rjobs[[rindex]])
                                rjobs[[rindex]] <- NA 
                                if (rindex == length(sjobs))
                                    break
                                else
                                    rindex <- rindex + 1
                            }
                        } else if (jindex == rindex) { 
                            while (sjobs[rindex] == "done") {
                                res <- REDUCE(res, rjobs[[rindex]])
                                rjobs[[rindex]] <- NA
                                if (rindex == length(sjobs))
                                    break
                                else
                                    rindex <- rindex + 1
                            }
                        }
                    ## reduce.in.order = FALSE 
                    } else if (!missing(REDUCE) && !reduce.in.order) {
                        if (length(res))
                            res <- REDUCE(res, unlist(status))
                        else
                            res <- unlist(status)
                    }
                }
            }

            ## fire a new job
            if (fire && !is.null(inextdata)) {
                jnodes[i] <- length(sjobs)+1
                sjobs[jnodes[i]] <- "running"
                pnodes[[i]] <- mcparallel(FUN(inextdata, ..., 
                                          chunkid=jnodes[i]), 
                                          mc.set.seed=TRUE)
                inextdata <- NULL
                break
            }

            ## no more jobs
            if (is.null(inextdata)) break
        }
    }

    if (missing(REDUCE))
        rjobs
    else
        list(res)
}
