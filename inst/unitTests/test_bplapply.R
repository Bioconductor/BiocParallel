library(foreach)
library(doParallel)
registerDoParallel(cores=2)
params <- list(mc=MulticoreParam(2),
               snow=SnowParam(type="FORK", workers=2),
               dopar=DoparParam())

x <- 1:10
expected <- lapply(x, sqrt)
for (ptype in names(params)) {
    checkIdentical(expected, bplapply(x, sqrt, param=params[[ptype]]), paste(ptype, "param works with bplapply"))
}
