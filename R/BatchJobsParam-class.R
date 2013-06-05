# TODO
# - add more parameters to control cluster / execution
# - more generics: bpvectorize, bpvectorize, bpmapply/bpmap
# - flag to turn progressbar off / reduce verbosity?
# - support chunking / write helper for all classes?
.BatchJobsParam <- setRefClass("BatchJobsParam",
    contains="BiocParallelParam",
    fields=list(
      reg.pars = "list",
      submit.pars = "list",
      cleanup = "logical",
      temp.dir = "character"
    ),
    methods=list(
      initialize = function(reg.pars, submit.pars, conf.pars, cleanup, temp.dir) {
        callSuper()
        if (!is.null(conf.pars$conffile))
          loadConfig(conf.pars$conffile)
        conf <- do.call(setConfig, conf.pars[setdiff(names(conf.pars), "conffile")])

        # TODO get number of workers for clusterfunctionsSSH and clusterFunctionsMulticore
        # other cluster functions: not possible
        # TODO is there a way to turn these build warnings off if a local
        # variable name matches a field name?
        # -> m/b always use ".[fieldname]"?
        n.workers <- NA_integer_
        if (conf$cluster.functions$name == "Interactive")
          n.workers <- 1L

        initFields(workers = n.workers, reg.pars = reg.pars,
                   submit.pars = submit.pars, cleanup = cleanup,
                   temp.dir = temp.dir)
      },
      show = function() {
        callSuper()
        # FIXME change to match output pattern of other classes
        print(getConfig())
      }))


BatchJobsParam <- function(cleanup = TRUE, temp.dir = getwd(), seed = NULL,
                           resources = NULL, conffile = NULL) {
  not_null <- Negate(is.null)
  reg.pars <- Filter(not_null, list(seed = seed))
  submit.pars <- Filter(not_null, list(resources = resources))
  conf.pars <- Filter(not_null, list(conffile = conffile))

  .BatchJobsParam(reg.pars = reg.pars, submit.pars = submit.pars,
                  conf.pars = conf.pars, cleanup = cleanup,
                  temp.dir = temp.dir)
}


## control

setMethod(bpschedule, "BatchJobsParam", function(x, ...) !getOption("BatchJobs.on.slave", FALSE))
setMethod(bpisup, "BatchJobsParam", function(x, ...) TRUE)

# FIXME ???
setMethod(bpbackend, "BatchJobsParam", function(x, ...) getConfig())

## evaluation

# move to utilities.R
.getErrorObj = function(val, msg) {
  attributes(val) <- list(class = "try-error", condition = msg)
  val
}

setMethod(bplapply, c("ANY", "BatchJobsParam"),
    function(X, FUN, ..., BPPARAM) {
    FUN <- match.fun(FUN)

    file.dir <- file.path(BPPARAM$temp.dir, tempfile("BiocParallel_tmp_", ""))
    pars <- c(list(id = "bplapply", file.dir = file.dir), BPPARAM$reg.pars)
    reg <- do.call("makeRegistry", pars)
    if (BPPARAM$cleanup)
      on.exit(unlink(file.dir, recursive = TRUE))

    ids <- batchMap(reg, FUN, X, more.args = list(...))
    pars <- c(list(reg = reg, ids = ids), BPPARAM$submit.pars)
    do.call(submitJobs, pars)
    waitForJobs(reg, timeout = Inf, stop.on.error = FALSE)

    error <- findErrors(reg)
    done <- findDone(reg)
    result <- vector("list", length(ids))
    result[done] <- loadResults(reg, done, use.names = FALSE)
    result[error] <- lapply(getErrors(reg, error, print = FALSE), .getErrorObj, val = NA)
    return(result)
})
