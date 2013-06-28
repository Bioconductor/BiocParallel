# TODO move this to a separate file
.lastError = setRefClass("lastError",
  fields = list(
    obj = "ANY",
    results = "list",
    error = "integer"),
  methods = list(
    initialize = function() {
      reset()
    },
    store = function(obj, results, error) {
      .self$obj = obj
      .self$results = results
      .self$error = error
      invisible(NULL)
    },
    reset = function() {
      .self$obj = NULL
      .self$results = list()
      .self$error = integer(0L)
      invisible(NULL)
    },
    isEmpty = function() {
      length(error) == 0L
    }
  )
)
lastError = .lastError()

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
    initialize = function(reg.pars, submit.pars, conf.pars, n.workers, cleanup, stop.on.error, progressbar) {
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
                            NA_integer_)
      }

      initFields(workers = n.workers, reg.pars = reg.pars, submit.pars = submit.pars,
                 conf.pars = new.conf, cleanup = cleanup, stop.on.error = stop.on.error,
                 progressbar = progressbar)
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

BatchJobsParam = function(workers = NULL, cleanup = TRUE, work.dir = getwd(), stop.on.error = TRUE, seed = NULL,
                           resources = NULL, conffile = NULL, cluster.functions = NULL, progressbar = TRUE) {
  not_null = Negate(is.null)
  reg.pars = Filter(not_null, list(seed = seed, work.dir = work.dir))
  submit.pars = Filter(not_null, list(resources = resources))
  conf.pars = Filter(not_null, list(conffile = conffile, cluster.functions = cluster.functions))

  .BatchJobsParam(reg.pars = reg.pars, submit.pars = submit.pars,
                  conf.pars = conf.pars, n.workers = workers, cleanup = cleanup,
                  stop.on.error = stop.on.error, progressbar = progressbar)
}


## control

setMethod(bpschedule, "BatchJobsParam", function(x, ...) !getOption("BatchJobs.on.slave", FALSE))
setMethod(bpisup, "BatchJobsParam", function(x, ...) TRUE)
setMethod(bpbackend, "BatchJobsParam", function(x, ...) getConfig())

## evaluation

setMethod(bplapply, c("ANY", "BatchJobsParam"),
    function(X, FUN, ..., BPPARAM) {
    FUN = match.fun(FUN)

    # turn progressbar on/off
    prev.pb = getOption("BBmisc.ProgressBar.style")
    options(BBmisc.ProgressBar.style = c("off", "text")[BPPARAM$progressbar+1L])
    on.exit(options(BBmisc.ProgressBar.style = prev.pb))

    # create registry, handle cleanup
    file.dir = file.path(BPPARAM$reg.pars$work.dir, tempfile("BiocParallel_tmp_", ""))
    pars = c(list(id = "bplapply", file.dir = file.dir, skip = FALSE), BPPARAM$reg.pars)
    reg = do.call("makeRegistry", pars)
    if (BPPARAM$cleanup)
      on.exit(unlink(file.dir, recursive = TRUE), add = TRUE)

    # switch config
    prev.config = getConfig()
    on.exit(do.call(setConfig, prev.config), add = TRUE)
    do.call(setConfig, BPPARAM$conf.pars)

    # check if X is error object
    recover = "lastError" %in% class(X)
    if (recover) {
      X = lastError$obj
      done = setdiff(seq_along(X), lastError$error)
    } else {
      done = integer(0L)
      lastError$reset()
    }

    # define jobs and submit
    ids = batchMap(reg, FUN, X, more.args = list(...))
    submitted = setdiff(ids, done)

    # submit, possibly chunked
    pars = c(list(reg = reg), BPPARAM$submit.pars)
    if (is.na(BPPARAM$workers))
      pars$ids = submitted
    else
      pars$ids = chunk(submitted, n.chunks = BPPARAM$workers, shuffle = TRUE)
    do.call(submitJobs, pars)
    all.done = waitForJobs(reg, submitted, timeout = Inf, stop.on.error = BPPARAM$stop.on.error)

    # if everything worked out fine we are done here and don't need the error handling overhead
    if (all.done) {
      if (!recover)
        return(loadResults(reg, submitted, use.names = FALSE))
      results = replace(lastError$results, lastError$error, loadResults(reg, submitted, use.names = FALSE))
      lastError$reset()
      return(results)
    }

    # construct result list
    results = vector("list", length(ids))
    ok = findDone(reg)
    error = setdiff(seq_along(ids), union(ok, done))
    results[done] = lastError$results[done]
    results[ok] = loadResults(reg, ids[ok], use.names = FALSE)
    results[error] = lapply(getErrorMessages(reg, ids[error]), simpleError)

    # safe mode: store partial results, kill jobs and raise error
    lastError$store(X, results, error)
    killJobs(reg, submitted)
    stop("Errors occurred during execution. First error message:\n",
         as.character(results[error][[1L]]),
         "You can resume calculation by re-calling 'bplapply' with 'lastError' as first argument.")
})
