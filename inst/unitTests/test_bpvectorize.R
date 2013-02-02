library(foreach)
library(doParallel)
registerDoParallel(cores=2)
params <- list(mc=MulticoreParam(2),
               snow=SnowParam(type="FORK", workers=2),
               dopar=DoparParam())

x <- 1:10
expected <- sqrt(x)
for (ptype in names(params)) {
    psqrt <- bpvectorize(sqrt, BPPARAM=params[[ptype]])
    checkIdentical(expected, psqrt(x),
                   paste(ptype, "BPPARAM works with bplapply"))
}
