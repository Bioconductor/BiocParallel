.BatchJobsParam <- setRefClass("BatchJobsParam",
  contains="BiocParallelParam",
  fields=list(
    reg.pars="list",
    submit.pars="list",
    conf.pars="list",
    cleanup="logical",
    stop.on.error="logical",
    progressbar="logical"
  ),

  methods=list(
    initialize = function(reg.pars, submit.pars, conf.pars, n.workers,
        catch.errors, cleanup, stop.on.error, progressbar, ...) {

        callSuper()

        ## save user config and reset it on exit
        prev.config <- getConfig()
        on.exit(do.call(setConfig, prev.config))
        if (!is.null(conf.pars$conffile))
            loadConfig(conf.pars$conffile)
        new.conf <- unclass(do.call(setConfig,
            conf.pars[setdiff(names(conf.pars), "conffile")]))

        if (is.null(n.workers)) {
          getNumberCPUs <- function(conf) {
              x <- environment(conf$cluster.functions$submitJob)$workers
              vapply(x, "[[", integer(1L), "ncpus")
          }
          cf.name <- new.conf$cluster.functions$name
          if (is.null(cf.name)) {
              n.workers <- NA
          } else {
              n.workers <- switch(cf.name,
                 "Multicore"=getNumberCPUs(new.conf),
                 "SSH"=sum(getNumberCPUs(new.conf)),
                 NA)
          }
        }
        n.workers <- as.integer(n.workers)

        initFields(workers=n.workers, catch.errors=catch.errors,
            reg.pars=reg.pars, submit.pars=submit.pars, conf.pars=new.conf,
            cleanup=cleanup, stop.on.error=stop.on.error,
            progressbar=progressbar)
    },

    show = function() {
        ## TODO more output
        callSuper()
        fields <- c("cleanup", "stop.on.error", "progressbar")
        vals <- c(.self$cleanup, .self$stop.on.error, .self$progressbar)
        txt <- paste(sprintf("%s: %s", fields, vals), collapse="; ")
        cat(strwrap(txt, exdent=2), sep="\n")
  })
)

BatchJobsParam <-
    function(workers=NULL, catch.errors=TRUE, cleanup=TRUE, work.dir=getwd(),
        stop.on.error=FALSE, seed=NULL, resources=NULL, conffile=NULL,
        cluster.functions=NULL, progressbar=TRUE, ...)
{
  ## FIXME:
  ## Would like the args to end with ... and these handled so a constructor of a
  ## derived class would work
  not_null <- Negate(is.null)
  reg.pars <- Filter(not_null, list(seed=seed, work.dir=work.dir))
  submit.pars <- Filter(not_null, list(resources=resources))
  conf.pars <- Filter(not_null,
      list(conffile=conffile, cluster.functions=cluster.functions))

  .BatchJobsParam(reg.pars=reg.pars, submit.pars=submit.pars,
      conf.pars=conf.pars, n.workers=workers, catch.errors=catch.errors,
      cleanup=cleanup, stop.on.error=stop.on.error, progressbar=progressbar)
}


## control

setMethod(bpschedule, "BatchJobsParam",
    function(x, ...)
{
    !getOption("BatchJobs.on.slave", FALSE)
})
setMethod(bpisup, "BatchJobsParam", function(x, ...) TRUE)
setMethod(bpbackend, "BatchJobsParam", function(x, ...) getConfig())

## evaluation

setMethod(bpmapply, c("ANY", "BatchJobsParam"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
        resume=getOption("BiocParallel.resume", FALSE), BPPARAM)
{
    FUN <- match.fun(FUN)
    if (resume)
        return(.resume(FUN=FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
            USE.NAMES=USE.NAMES, BPPARAM=BPPARAM))
    if (!bpschedule(BPPARAM))
        return(bpmapply(FUN=FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
            USE.NAMES=USE.NAMES, resume=resume,
            BPPARAM=SerialParam(catch.errors=BPPARAM$catch.errors)))

    # turn progressbar on/off
    prev.pb <- getOption("BBmisc.ProgressBar.style")
    options(BBmisc.ProgressBar.style=c("off", "text")[BPPARAM$progressbar+1L])
    on.exit(options(BBmisc.ProgressBar.style=prev.pb))

    # create registry, handle cleanup
    file.dir <- file.path(BPPARAM$reg.pars$work.dir,
        tempfile("BiocParallel_tmp_", ""))
    pars <- c(list(id="bpmapply", file.dir=file.dir, skip=FALSE),
        BPPARAM$reg.pars)
    reg <- suppressMessages(do.call("makeRegistry", pars))
    if (BPPARAM$cleanup)
        on.exit(unlink(file.dir, recursive=TRUE), add=TRUE)

    # switch config
    prev.config <- getConfig()
    on.exit(setConfig(conf=prev.config), add=TRUE)
    setConfig(conf=BPPARAM$conf.pars)

    # quick sanity check
    if (BPPARAM$stop.on.error && BPPARAM$catch.errors)
      warning(paste(
          "Options 'stop.on.error' and 'catch.errors' are both set, but are mutually exclusive.",
          "Disabling stop.on.error."))

    # define jobs and submit
    if (is.null(MoreArgs))
        MoreArgs <- list()
    if (BPPARAM$catch.errors)
        FUN <- .composeTry(FUN)
    ids <- suppressMessages(batchMap(reg, fun=FUN, ..., more.args=MoreArgs))

    # submit, possibly chunked
    pars <- c(list(reg=reg), BPPARAM$submit.pars)
    if (is.na(BPPARAM$workers))
        pars$ids <- ids
    else
        pars$ids <- chunk(ids, n.chunks=BPPARAM$workers, shuffle=TRUE)
    suppressMessages(do.call(submitJobs, pars))

    # wait for the jobs to terminate
    waitForJobs(reg, ids, timeout=Inf,
        stop.on.error=(BPPARAM$stop.on.error && !BPPARAM$catch.errors))

    # identify missing results
    ok <- ids %in% suppressMessages(findDone(reg))
    if (BPPARAM$catch.errors) {
        results <- loadResults(reg, ids, use.names="none", missing.ok=TRUE)
        ok <- ok & !vapply(results, inherits, logical(1L), what="remote-error")
    }

    # handle errors
    if (!all(ok)) {
      if (BPPARAM$catch.errors) {
          # already fetched the results ...
          results <- .rename(results, list(...), USE.NAMES=USE.NAMES)
          LastError$store(results=results, is.error=!ok, throw.error=TRUE)
      }
      stop(simpleError(as.character(getErrorMessages(reg, head(ids[!ok], 1L)))))
    }

    if (!BPPARAM$catch.errors) { # results not fetched yet
        results <- loadResults(reg, ids, use.names="none")
    }

    .simplify(.rename(results, list(...), USE.NAMES=USE.NAMES), SIMPLIFY=SIMPLIFY)
})
