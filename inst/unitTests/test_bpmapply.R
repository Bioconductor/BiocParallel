.fork_not_windows <- 
    function(expected, expr) 
{
    err <- NULL
    obs <- tryCatch(expr, error=function(e) {
        if (!all(grepl("fork clusters are not supported on Windows",
                       conditionMessage(e))))
            err <<- conditionMessage(e)
        expected
    })
    checkTrue(is.null(err))
    checkIdentical(expected, obs)
}

test_bpmapply_Params <- function() {
    params <- list(serial=SerialParam(),
                  snow0=SnowParam(2, "FORK"),
                  snow1=SnowParam(2, "PSOCK"),
                  batchjobs=BatchJobsParam(),
                  multi=MulticoreParam(),
                  dopar=DoparParam())

    x <- 1:10
    y <- rev(x)
    f <- function(x, y) x + y
    expected <- x + y
    for (param in params) {
      .fork_not_windows(expected, bpmapply(f, x, y, BPPARAM=param))
    }

    # test names and simplify
    x <- setNames(1:5, letters[1:5])
    for (param in params) {
        for (catch.errors in c(FALSE, TRUE)) {
            param$catch.errors <- catch.errors
            for (SIMPLIFY in c(FALSE, TRUE)) {
                for (USE.NAMES in c(FALSE, TRUE)) {
                  expected <- mapply(identity, x, USE.NAMES=USE.NAMES, 
                      SIMPLIFY=SIMPLIFY)
                  .fork_not_windows(expected, 
                      bpmapply(identity, x, USE.NAMES=USE.NAMES, 
                          SIMPLIFY=SIMPLIFY, BPPARAM=param))
                }
            }
        }
    }


    # test MoreArgs
    x <- setNames(1:5, letters[1:5])
    f <- function(x, m) { x + m }
    expected <- mapply(f, x, MoreArgs=list(m=1))
    for (param in params) {
      .fork_not_windows(expected, 
          bpmapply(f, x, MoreArgs=list(m=1), BPPARAM=param))
    }

    TRUE
}
