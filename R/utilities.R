.splitIndices <- function (nx, ncl)
{
    ## derived from parallel
    i <- seq_len(nx)
    if (ncl <= 1L || nx <= 1L)          # allow nx, nc == 0
        i
    else {
        fuzz <- min((nx - 1L)/1000, 0.4 * nx/ncl)
        breaks <- seq(1 - fuzz, nx + fuzz, length = ncl + 1L)
        structure(split(i, cut(i, breaks)), names = NULL)
    }
}

tryCatchDebug = function(expr) {
  # variables used in subfunctions
  catched.error = FALSE
  cache.warnings = vector("list", 0L)

  # handler to cache the warning messages
  handler_warning = function(w) {
    cache.warnings <<- c(cache.warnings, w)
    invokeRestart("muffleWarning")
  }

  # handler to handle errors:
  # set flag, retrieve dump frame, get deparsed sys.calls and call abort handler
  handler_error = function(e) {
    catched.error <<- TRUE
    dump.frames(".lastDump")
    dumped = get(".lastDump", envir = .GlobalEnv)
    rm(".lastDump", envir = .GlobalEnv)
    call = sapply(sys.calls(), deparse)
    invokeRestart("abort", e, call, dumped)
  }

  # handler for aborts: actually only returns collected data as a named list
  handler_abort = function(e, call, dumped) {
    list(value = e, call = call, dump = dumped)
  }


  # evaluate expression
  x = withRestarts(withCallingHandlers(expr, warning = handler_warning, error = handler_error), abort = handler_abort)

  # insert data into a named list and return
  ret = list(value = NULL,
             warnings = cache.warnings,
             call = NULL,
             dump = NULL,
             is.error = catched.error,
             has.warnings = (length(cache.warnings) > 0L))

  if (catched.error) {
    ret[names(x)] = x
    class(ret) = "try-error"
  } else {
    ret$value = x
  }

  return(ret)
}
