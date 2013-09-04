.BatchJobsParam = setRefClass("BatchJobsParam",
  contains="BiocParallelParam",
  fields=list(
    reg.pars = "list",
    submit.pars = "list",
    conf.pars = "list",
    cleanup = "logical",
    stop.on.error = "logical",
    progressbar = "logical"
  ),

  methods=list(
    initialize = function(reg.pars, submit.pars, conf.pars, n.workers, catch.errors, store.dump, cleanup, stop.on.error, progressbar) {
      callSuper()

      # save user config and reset it on exit
      prev.config = getConfig()
      on.exit(do.call(setConfig, prev.config))
      if (!is.null(conf.pars$conffile))
        loadConfig(conf.pars$conffile)
      new.conf = unclass(do.call(setConfig, conf.pars[setdiff(names(conf.pars), "conffile")]))

      if (is.null(n.workers)) {
        getNumberCPUs = function(conf) {
          x = environment(conf$cluster.functions$submitJob)$workers
          vapply(x, "[[", integer(1L), "ncpus")
        }
        n.workers = switch(new.conf$cluster.functions$name,
                           "Multicore" = getNumberCPUs(new.conf),
                           "SSH" = sum(getNumberCPUs(new.conf)),
                           1L)
      }

      initFields(workers = n.workers, catch.errors = catch.errors, reg.pars = reg.pars,
                 submit.pars = submit.pars, conf.pars = new.conf, cleanup = cleanup,
                 stop.on.error = stop.on.error, progressbar = progressbar)
    },

    show = function() {
      # TODO more output
      callSuper()
      fields = c("cleanup", "stop.on.error", "progressbar")
      vals = c(.self$cleanup, .self$stop.on.error, .self$progressbar)
      txt = paste(sprintf("%s: %s", fields, vals), collapse="; ")
      cat(strwrap(txt, exdent=2), sep="\n")
  })
)

BatchJobsParam = function(workers = NULL, catch.errors = FALSE, store.errors = FALSE, cleanup = TRUE, work.dir = getwd(), stop.on.error = FALSE, seed = NULL,
                           resources = NULL, conffile = NULL, cluster.functions = NULL, progressbar = TRUE) {
  not_null = Negate(is.null)
  reg.pars = Filter(not_null, list(seed = seed, work.dir = work.dir))
  submit.pars = Filter(not_null, list(resources = resources))
  conf.pars = Filter(not_null, list(conffile = conffile, cluster.functions = cluster.functions))

  .BatchJobsParam(reg.pars = reg.pars, submit.pars = submit.pars,
                  conf.pars = conf.pars, n.workers = workers, catch.errors = catch.errors,
                  store.dump = store.dump, cleanup = cleanup, stop.on.error = stop.on.error, 
                  progressbar = progressbar)
}


## control

setMethod(bpschedule, "BatchJobsParam", function(x, ...) !getOption("BatchJobs.on.slave", FALSE))
setMethod(bpisup, "BatchJobsParam", function(x, ...) TRUE)
setMethod(bpbackend, "BatchJobsParam", function(x, ...) getConfig())

## evaluation

setMethod(bplapply, c("ANY", "BatchJobsParam"),
    function(X, FUN, ..., BPPARAM) {
    FUN = match.fun(FUN)

    if (!bpschedule(BPPARAM))
        return(lapply(X, FUN, ...))

    # turn progressbar on/off
    prev.pb = getOption("BBmisc.ProgressBar.style")
    options(BBmisc.ProgressBar.style = c("off", "text")[BPPARAM$progressbar+1L])
    on.exit(options(BBmisc.ProgressBar.style = prev.pb))

    # create registry, handle cleanup
    file.dir = file.path(BPPARAM$reg.pars$work.dir, tempfile("BiocParallel_tmp_", ""))
    pars = c(list(id = "bplapply", file.dir = file.dir, skip = FALSE), BPPARAM$reg.pars)
    reg = suppressMessages(do.call("makeRegistry", pars))
    if (BPPARAM$cleanup)
      on.exit(unlink(file.dir, recursive = TRUE), add = TRUE)

    # switch config
    prev.config = getConfig()
    on.exit(do.call(setConfig, prev.config), add = TRUE)
    do.call(setConfig, BPPARAM$conf.pars)

    # define jobs and submit
    ids = batchMap(reg, FUN, X, more.args = list(...))

    # submit, possibly chunked
    pars = c(list(reg = reg), BPPARAM$submit.pars)
    if (is.na(BPPARAM$workers))
      pars$ids = ids
    else
      pars$ids = chunk(ids, n.chunks = BPPARAM$workers, shuffle = TRUE)
    suppressMessages(do.call(submitJobs, pars))
    all.done = waitForJobs(reg, ids, timeout = Inf, stop.on.error = BPPARAM$stop.on.error)

    # if everything worked out fine we are done here and don't need the error handling overhead
    if (all.done) {
      return(loadResults(reg, ids, use.names = FALSE))
    } else if (BPPARAM$catch.errors) {
      ok = ids %in% findDone(reg)
      results = vector("list", length(ids))
      results[ok] = loadResults(reg, ids[ok], use.names = FALSE)
      results[!ok] = lapply(getErrorMessages(reg, ids[!ok]), function(msg) simpleError(as.character(msg)))
      LastError$store(obj = X, results = results, is.error = !ok, throw.error = TRUE)
    } else {
      stop(simpleError(as.character(getErrorMessages(reg, head(findErrors(reg), 1L)))))
    }
})



setMethod(bpmapply, c("ANY", "BatchJobsParam"),
  function(FUN, ..., MoreArgs = NULL, SIMPLIFY = TRUE, USE.NAMES = TRUE, BPPARAM) {
    FUN <- match.fun(FUN)
    if (!bpschedule(BPPARAM))
        return(bpmapply(FUN=FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY, USE.NAMES=USE.NAMES, BPPARAM=SerialParam()))

    # turn progressbar on/off
    prev.pb = getOption("BBmisc.ProgressBar.style")
    options(BBmisc.ProgressBar.style = c("off", "text")[BPPARAM$progressbar+1L])
    on.exit(options(BBmisc.ProgressBar.style = prev.pb))

    # create registry, handle cleanup
    file.dir = file.path(BPPARAM$reg.pars$work.dir, tempfile("BiocParallel_tmp_", ""))
    pars = c(list(id = "bplapply", file.dir = file.dir, skip = FALSE), BPPARAM$reg.pars)
    reg = suppressMessages(do.call("makeRegistry", pars))
    if (BPPARAM$cleanup)
      on.exit(unlink(file.dir, recursive = TRUE), add = TRUE)

    # switch config
    prev.config = getConfig()
    on.exit(do.call(setConfig, prev.config), add = TRUE)
    do.call(setConfig, BPPARAM$conf.pars)

    # define jobs and submit
    if (is.null(MoreArgs))
      MoreArgs = list()
    ids = batchMap(reg, fun = FUN, ..., more.args = MoreArgs)

    # submit, possibly chunked
    pars = c(list(reg = reg), BPPARAM$submit.pars)
    if (is.na(BPPARAM$workers))
      pars$ids = ids
    else
      pars$ids = chunk(ids, n.chunks = BPPARAM$workers, shuffle = TRUE)
    suppressMessages(do.call(submitJobs, pars))
    all.done = waitForJobs(reg, ids, timeout = Inf, stop.on.error = BPPARAM$stop.on.error)

    if (!all.done)
      stop(simpleError(as.character(getErrorMessages(reg, head(findErrors(reg), 1L)))))

    results = loadResults(reg, ids, use.names = FALSE)
    if (USE.NAMES) {
      dots = list(...)
      if (length(dots)) {
        if (is.null(names(dots[[1L]]))) {
            if(is.character(dots[[1L]]))
              names(results) = dots[[1L]]
        } else {
          names(results) = names(dots[[1L]])
        }
      }
    }

    if (SIMPLIFY)
      return(simplify2array(results))
    return(results)
})
