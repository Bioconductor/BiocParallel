## This multi-core implementation of bpiterate() is a modified
## version of sclapply() by Gregoire Pau.

.bpiterate <- function(ITER, FUN, ...,
    mc.set.seed = TRUE, mc.silent = FALSE, 
    mc.cores = getOption("mc.cores", 2L),
    mc.cleanup = TRUE)
{
    ## initialize scheduler
    sjobs <- character(0)                   ## jobs (state)
    rjobs <- list()                         ## jobs (result)
    pnodes <- vector(mode="list", mc.cores) ## nodes (process)
    jnodes <- rep(NA, mc.cores)             ## nodes (job id)

    ## cleanup procedure, based on mclapply
    on.exit(.cleanup(pnodes[!sapply(pnodes, is.null)], mc.cleanup))

    ## start scheduler
    collect.timeout <- 2 ## 2 seconds wait between each iteration
    inextdata <- NULL
    i <- 0
    repeat {
        ## is there a new job to process?
        if (is.null(inextdata)) inextdata <- ITER()
        ## are all the jobs done?
        if (is.null(inextdata)) {
          if (length(sjobs)==0) break ## no jobs have been run
          if (all(sjobs=="done")) break
        }

        ## fire FUN(inextdata) on node i
        repeat {
            i <- (i %% length(pnodes))+1
            process <- pnodes[[i]]
            if (!is.null(process)) {
              ## wait collect.timeout seconds        
              status <- mccollect(process, wait=FALSE, timeout=collect.timeout) 
              if (is.null(status)) {
                ## node busy
                fire <- FALSE
              } else {
                ## node done: save results and fire a new job
                mccollect(process) ## kill job
                rjobs[jnodes[i]] <- status
                sjobs[jnodes[i]] <- "done"
              }
            } else {
              ## virgin node
              fire <- TRUE
            }

            ## fire a new job
            if (fire && !is.null(inextdata)) {
              jnodes[i] <- length(sjobs)+1
              sjobs[jnodes[i]] <- "running"
              pnodes[[i]] <- mcparallel(FUN(inextdata, ..., 
                                        chunkid=jnodes[i]), mc.set.seed=TRUE)
              inextdata <- NULL
              break
            }

            ## no more jobs
            if (is.null(inextdata)) break
        }
    }

    rjobs
}
