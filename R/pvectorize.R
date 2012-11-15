pvectorize <- function(FUN, vectorize.args = arg.names) {
    ## This works like the real formals function, but it also works on
    ## most primitives where formals does not.
    formals <- function(fun) as.pairlist(head(as.list(args(fun)), -1))

    arg.names <- as.list(formals(FUN))
    if (is.null(arg.names)) {
        stop("Cannot determine argument names for FUN.")
    }
    arg.names[["..."]] <- NULL
    arg.names <- names(arg.names)
    vectorize.args <- as.character(vectorize.args)

    mcformals <- formals(function(mc.set.seed = TRUE, mc.silent = FALSE,
                                  mc.cores = getOption("mc.cores", 2L), mc.cleanup = TRUE,
                                  mc.preschedule=FALSE, mc.num.chunks, mc.chunk.size) NULL)

    if (!all(vectorize.args %in% arg.names))
        stop("must specify formal argument names to vectorize")

    if (!length(vectorize.args)) {
        warning("Not parallelizing any arguments")
        ## The "mc.*" arguments will stil be added to FUN's arg list
        ## for compatibility, but they will have no effect.
        FUNPV <- FUN
    } else {
        FUNPV <- function() {
            args <- lapply(as.list(match.call())[-1L], eval, parent.frame())
            ## Split into mc.args and args for FUN. Techincally they can
            ## overlap if FUN accepts args with names like "mc.cores", but
            ## that's a bad idea.
            mc.args <- args[names(args) %in% names(mcformals)]
            args <- args[names(args) %in% names(formals(FUN))]
            names <- if (is.null(names(args)))
              character(length(args))
            else names(args)
            dovec <- names %in% vectorize.args
            n <- max(sapply(args[dovec], length))
            for (arg in args[dovec]) {
                if (n %% length(arg) != 0) {
                    warning("longer argument not a multiple of length of shorter")
                    break
                }
            }
            cores <- as.integer(mc.cores)
            if(cores < 1L) stop("'mc.cores' must be >= 1")
            if(cores == 1L) return(do.call(FUN, args))

            if(mc.set.seed) mc.reset.stream()

            if (missing(mc.num.chunks)) {
                if (missing(mc.chunk.size)) {
                    mc.num.chunks <- cores
                    mc.preschedule <- TRUE
                } else {
                    mc.num.chunks <- ceiling(n/mc.chunk.size)
                }
            }
            if (mc.num.chunks > 1) {
                si <- tryCatch(splitIndices(n, mc.num.chunks),
                               error=function(...) list(1:n))
            } else {
                si <- list(1:n)
            }
            mclapply.extra.args <- mc.args[names(mc.args) %in% names(formals(mclapply))]
            reslist <- do.call(mclapply,
                               c(list(X=si, FUN=function(i)
                                      do.call(FUN, c(lapply(args[dovec], function(vec) vec[(i-1) %% length(vec) + 1]),
                                                     args[!dovec]))),
                                 mclapply.extra.args))
            res <- do.call(c, reslist)
            if (length(res) != n)
              warning("some results may be missing, folded or caused an error")
            res
        }
    }
    funformals <- formals(FUN)
    funformals <- c(funformals, mcformals[! names(mcformals) %in% names(funformals)])
    formals(FUNPV) <- as.pairlist(funformals)
    FUNPV
}
