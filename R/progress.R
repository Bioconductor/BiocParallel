### =========================================================================
### progress bar 
### -------------------------------------------------------------------------

## derived from plyr::progress_text()
.progress <- function(style = 3, active = TRUE, ...) {
    ntasks <- 0
    txt <- NULL
    max <- 0

    if (active) {
        list(
            init = function(x) {
                txt <<- txtProgressBar(max = x, style = style, ...)
                setTxtProgressBar(txt, 0)
                max <<- x
            },
            step = function() {
                ntasks <<- ntasks + 1
                setTxtProgressBar(txt, ntasks)
                if (ntasks == max) cat("\n")
            },
            term = function() close(txt)
        )
    } else {
        list(
            init = function(x) NULL,
            step = function() NULL, 
            term = function() NULL
        )
    }
}
