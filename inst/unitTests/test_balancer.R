.lazyCount <- function(count) {
    i <- 0L
    function() {
        if (i >= count)
            return(NULL)
        i <<- i + 1L
        i
    }
}

test_balancer_lapply <-
    function()
{
    params <- list(
        SerialParam(),
        SnowParam(2)
    )
    for (p in params) {
        bptasks(p) <- 10
        opts <- bpoptions(lapplyBalancer = "sequential")
        res1 <- bplapply(1:10, function(x) x, BPPARAM = p,
                         BPOPTIONS = opts)

        opts <- bpoptions(lapplyBalancer = "random")
        res2 <- bplapply(1:10, function(x) x, BPPARAM = p,
                         BPOPTIONS = opts)
        checkIdentical(res1, res2)
    }
}

test_balancer_lapply_rng <-
    function()
{
    params <- list(
        SerialParam(),
        SnowParam(2)
    )
    for (p in params) {
        bptasks(p) <- 10
        bpRNGseed(p) <- 123
        opts <- bpoptions(lapplyBalancer = "sequential")
        res1 <- bplapply(1:10, function(x)runif(1), BPPARAM = p,
                         BPOPTIONS = opts)

        bpRNGseed(p) <- 123
        opts <- bpoptions(lapplyBalancer = "random")
        res2 <- bplapply(1:10, function(x)runif(1), BPPARAM = p)
        checkIdentical(res1, res2)
    }
}

test_balancer_lapply_rng_redo <-
    function()
{
    foo1 <- function(x) runif(1)
    errorIdx <- c(2,3,6)
    foo2 <- function(x){
        stopifnot(!x%in%errorIdx)
        runif(1)
    }
    answer <- bplapply(1:10, foo1, BPPARAM = SerialParam(RNGseed = 123))
    params <- list(
        SerialParam(),
        SnowParam(2)
    )
    balancerTypes <- c("sequential", "random")
    taskNums <- c(1, 2, 5, 10)
    for (p in params) {
        for(balancerType in balancerTypes){
            for(taskNum in taskNums){
                opts <- bpoptions(lapplyBalancer = balancerType,
                                  tasks = taskNum,
                                  RNGseed = 123,
                                  stop.on.error = FALSE)
                res1 <- bptry(bplapply(1:10, foo2, BPPARAM = p,
                                       BPOPTIONS = opts))
                checkIdentical(answer[-errorIdx], res1[-errorIdx])
                checkIdentical(bpok(res1)[errorIdx],
                               rep(FALSE, length(errorIdx)))

                res2 <- bplapply(1:10, foo1, BPPARAM = p,
                                 BPREDO = res1,
                                 BPOPTIONS = opts)
                checkIdentical(answer, res2)
            }
        }
    }
}

test_balancer_iterate <-
    function()
{
    params <- list(
        SerialParam(),
        SnowParam(2)
    )
    for (p in params) {
        bptasks(p) <- 10
        res1 <- bpiterate(.lazyCount(10), function(x) x, BPPARAM = p)

        opts <- bpoptions(iterateBalancer = "sequential")
        res2 <- bplapply(1:10, function(x) x, BPPARAM = p)
        checkIdentical(res1, res2)
    }
}

test_balancer_lapply_rng <-
    function()
{
    params <- list(
        SerialParam(),
        SnowParam(2)
    )
    for (p in params) {
        bptasks(p) <- 10
        bpRNGseed(p) <- 123
        res1 <- bpiterate(.lazyCount(10), function(x)runif(1), BPPARAM = p)

        bpRNGseed(p) <- 123
        res2 <- bplapply(1:10, function(x)runif(1), BPPARAM = p)
        checkIdentical(res1, res2)
    }
}

test_balancer_iterate_rng_redo <-
    function()
{
    foo1 <- function(x) runif(1)
    errorIdx <- c(2,3,6)
    foo2 <- function(x){
        stopifnot(!x%in%errorIdx)
        runif(1)
    }
    answer <- bplapply(1:10, foo1, BPPARAM = SerialParam(RNGseed = 123))
    params <- list(
        SerialParam(),
        SnowParam(2)
    )
    for (p in params) {
        opts <- bpoptions(RNGseed = 123,
                          stop.on.error = FALSE)
        res1 <- bptry(bpiterate(.lazyCount(10), foo2, BPPARAM = p,
                               BPOPTIONS = opts))
        checkIdentical(answer[-errorIdx], res1[-errorIdx])
        checkTrue(sum(sapply(res1[errorIdx], is.null)) == 3)

        res2 <- bpiterate(.lazyCount(10), foo1, BPPARAM = p,
                         BPREDO = res1,
                         BPOPTIONS = opts)
        checkIdentical(answer, res2)
    }
}