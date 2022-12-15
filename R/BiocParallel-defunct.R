bprunMPIslave <- function() {
    .Defunct("bprunMPIworker")
}

BatchJobsParam <-
    function(workers=NA_integer_, cleanup=TRUE,
        work.dir=getwd(), stop.on.error=TRUE, seed=NULL, resources=NULL,
        conffile=NULL, cluster.functions=NULL,
        progressbar=TRUE, jobname = "BPJOB",
        timeout = WORKER_TIMEOUT,
        reg.pars=list(seed=seed, work.dir=work.dir),
        conf.pars=list(conffile=conffile, cluster.functions=cluster.functions),
        submit.pars=list(resources=resources), ...)
{
    .Defunct("BatchtoolsParam")
}
