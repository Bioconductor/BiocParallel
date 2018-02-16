library(batchtools)

pi <- function(n, ...) {
    nums <- matrix(runif(2 * n), ncol = 2)
    d <- sqrt(nums[, 1]^2 + nums[, 2]^2)
    4 * mean(d <= 1)
}

n_nodes <- 2

reg <- makeRegistry(
    file.dir = tempfile("registry_", "."),
    conf.file = "inst/batchtools.Multicore_conf.R"
)

## cluster.functions <- makeClusterFunctionsSGE("batchtools-sge.tmpl")

ids <- batchMap(fun = pi, n = rep(1e5, n_nodes * 4), reg = reg)
submitJobs(ids, reg = reg)
waitForJobs(reg = reg)
result <- lapply(seq_len(n_nodes * 4), loadResult, reg = reg)

clearRegistry(reg = reg)

## another version
## jobs <- seq_len(10 * n_nodes)
## result <- btlapply(jobs, pi, n = 1e5, n.chunks = n_nodes, reg = reg)
## clearRegistry(reg)

param <- BatchtoolsParam(
    workers = batchtoolsWorkers(),      # ncpus
    conf.file = batchtoolsConf()        # linux / mac: multicore; Windows: ?socket?
)

bpstart(param)                          # makeRegistry(), add registry to param
bplapply(X, FUN, BPPARAM = param)
## batchMap()
## submitJobs()
## waitForJobs()
## result <- lapply(X, loadResult)
## clearRegistry()
bpstop(param)                           # removeRegistry(), remove registry from param

## Analogous behavior to SnowParam()
param <- SnowParam(2)
bpstart(param)                          # see param$cluster before and after bpstart()
bplapply(X, FUN, BPPARAM=param)
bpstop(param)
