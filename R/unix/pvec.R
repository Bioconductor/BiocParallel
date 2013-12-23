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

pvec <- function(v, FUN, ..., AGGREGATE=c,
                 mc.set.seed = TRUE, mc.silent = FALSE,
                 mc.cores = getOption("mc.cores", 2L), mc.cleanup = TRUE,
                 mc.preschedule=FALSE, num.chunks, chunk.size)
{
    env <- parent.frame()
    cores <- as.integer(mc.cores)
    if(cores < 1L) stop("'mc.cores' must be >= 1")
    if(cores == 1L) return(FUN(v, ...))

    if(mc.set.seed) mc.reset.stream()

    n <- length(v)
    if (missing(num.chunks)) {
        if (missing(chunk.size)) {
            num.chunks <- cores
            mc.preschedule <- TRUE
        } else {
            num.chunks <- ceiling(n/chunk.size)
        }
    }
    si <- splitIndices(n, num.chunks)
    res0 <- mclapply(si, function(i) FUN(v[i], ...),
                     mc.set.seed=mc.set.seed,
                     mc.silent=mc.silent,
                     mc.cores=mc.cores,
                     mc.cleanup=mc.cleanup,
                     mc.preschedule=mc.preschedule)
    do.call(AGGREGATE, res0)
}
