#  File src/library/parallel/R/unix/pvec.R
#  Part of the R package, http://www.R-project.org
#
#  Copyright (C) 1995-2012 The R Core Team
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  A copy of the GNU General Public License is available at
#  http://www.r-project.org/Licenses/

### Derived from parallel version 2.16.0 by R Core Team
### Derived from multicore version 0.1-6 by Simon Urbanek

pvec <- function(v, FUN, ..., mc.set.seed = TRUE, mc.silent = FALSE,
                 mc.cores = getOption("mc.cores", 2L), mc.cleanup = TRUE,
                 mc.preschedule=FALSE, mc.num.chunks, mc.chunk.size)
{
    ## Don't vectorize if arg is a scalar
    if (length(v) <= 1)
        return(FUN(v, ...))
    cores <- as.integer(mc.cores)
    if (cores < 1L)
        stop("'mc.cores' must be >= 1")
    ## Don't use more cores than there are elements in v
    if (cores > length(v))
        cores <- length(v)
    n <- length(v)
    if (missing(mc.num.chunks)) {
        if (missing(mc.chunk.size)) {
            mc.num.chunks <- cores
            mc.preschedule <- TRUE
        } else {
            mc.num.chunks <- ceiling(n/mc.chunk.size)
        }
    }
    ## Don't allow more chunks than there are elements in v
    if (mc.num.chunks > length(v))
        mc.num.chunks <- length(v)
    ## If only one chunk, don't attempt parallelism
    if (mc.num.chunks == 1L)
        return(FUN(v, ...))
    si <- splitIndices(n, mc.num.chunks)
    res <- do.call(c,
                   mclapply(si, function(i) FUN(v[i], ...),
                            mc.set.seed=mc.set.seed,
                            mc.silent=mc.silent,
                            mc.cores=mc.cores,
                            mc.cleanup=mc.cleanup,
                            mc.preschedule=mc.preschedule))
    if (length(res) != n)
        warning("some results may be missing, folded or caused an error")
    res
}
