### =========================================================================
### progress bar 
### -------------------------------------------------------------------------

.progress <- function(style = 3, active = TRUE, iterate = FALSE, ...) {

    if (active) {
        ntasks <- 0L
        if (iterate) {
            list(init = function(x) {
                message("iteration: ", appendLF=FALSE)
            }, step = function(n) {
                ntasks <<- ntasks + 1L
                erase <- paste(rep("\b", ceiling(log10(ntasks))), collapse="")
                message(erase, ntasks, appendLF = FALSE)
            }, term = function() {
                message()               # new line
            })
        } else {
            ## derived from plyr::progress_text()
            txt <- NULL
            max <- 0
            list(init = function(x) {
                txt <<- txtProgressBar(max = x, style = style, ...)
                setTxtProgressBar(txt, 0)
                max <<- x
            }, step = function(n) {
                ntasks <<- ntasks + n
                setTxtProgressBar(txt, ntasks)
                if (ntasks == max) cat("\n")
            }, term = function() {
                close(txt)
            })
        }
    } else {
        list(
            init = function(x) NULL,
            step = function(n) NULL,
            term = function() NULL
        )
    }
}
