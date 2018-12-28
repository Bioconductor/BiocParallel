\name{BatchJobsParam-class}
\Rdversion{1.1}
\docType{class}

\alias{BatchJobsParam-class}
\alias{BatchJobsParam}

\alias{bpbackend,BatchJobsParam-method}
\alias{bpbackend<-,BatchJobsParam}
\alias{bpisup,BatchJobsParam-method}
\alias{bpstart,BatchJobsParam-method}
\alias{bpstop,BatchJobsParam-method}
\alias{bpworkers,BatchJobsParam-method}
\alias{bpschedule,BatchJobsParam-method}

\alias{show,BatchJobsParam-method}

\title{Enable parallelization on batch systems}

\description{

    This class is used to parameterize scheduler options on managed
    high-performance computing clusters.

}

\usage{
BatchJobsParam(workers, cleanup = TRUE, 
    work.dir = getwd(), stop.on.error = TRUE, seed = NULL,
    resources = NULL, conffile = NULL, cluster.functions = NULL, 
    progressbar = TRUE, jobname = "BPJOB",
    reg.pars=list(seed=seed, work.dir=work.dir),
    conf.pars=list(conffile=conffile, cluster.functions=cluster.functions),
    submit.pars=list(resources=resources),
    ...)
}

\arguments{

    \item{workers}{\code{integer(1)}} Number of workers to divide tasks
        (e.g., elements in the first argument of \code{bplapply})
        between. On Multicore and SSH backends, this defaults to all
        available nodes.  On managed (e.g., slurm, SGE) clusters
        \code{workers} defaults to \code{NA}, meaning that the number of
        workers equals the number of tasks.  See argument
        \code{n.chunks} in \code{\link[BBmisc]{chunk}} and
        \code{\link[BatchJobs]{submitJobs}} for more information.

    \item{cleanup}{\code{logical(1)}} BatchJobs creates temporary
        directories in the \code{work.dir}. If \code{cleanup} is set to
        \code{TRUE} (default), the directories are removed from the file
        systems automatically. Set this to \code{FALSE} whenever it
        might become necessary to utilize any special functionality
        provided by BatchJobs. To retrieve the registry, call
        \code{\link[BatchJobs]{loadRegistry}} on the temporary
        directory.

    \item{work.dir}{\code{character(1)}} Directory to store temporary
        files. Note that this must be shared across computational nodes
        if you use a distributed computing backend. Default ist the
        current working directory of \code{R}, see
        \code{\link[base]{getwd}}. Ignored when \code{reg.pars} is
        provided.

    \item{stop.on.error}{\code{logical(1)}} Stop all jobs as soon as one
        jobs fails (\code{stop.on.error == TRUE}) or wait for all jobs
        to terminate.  Default is \code{TRUE}.

    \item{seed}{\code{integer(1L)}} Set an initial seed for the RNG. See
        \code{\link[BatchJobs]{makeRegistry}} for more information.
        Default is \code{NULL} where a random seed is chosen upon
        initialization. Ignored when \code{reg.pars} is provided.

    \item{resources}{\code{list()}} List of job specific resources
        passed to \code{\link[BatchJobs]{submitJobs}}.  Default is
        \code{NULL} where the resources defined in the configuration are
        used. Ignored when \code{submit.pars} is provided.

    \item{conffile}{\code{character(1)}} URI to a custom BatchJobs
        configuration file used for execution.  Default is \code{NULL}
        which relies on BatchJobs to handle configuration files. Ignored
        when \code{conf.pars} is provided.

    \item{cluster.functions}{ClusterFunctions} Specify a specific
        cluster backend using on of the constructors provided by
        BatchJobs, see
        \link[BatchJobs:makeClusterFunctions]{ClusterFunctions}.
        Default is \code{NULL} where the default cluster functions
        defined in the configuration are used. Ignored when
        \code{conf.pars} is provided.

    \item{progressbar}{\code{logical(1)}}
        Suppress the progress bar used in BatchJobs and be less verbose.
        Default is \code{FALSE}.

   \item{jobname}{\code{character(1)}} Job name that is prepended
       to the output log and result files. Default is "BPJOB".

   \item{reg.pars}{\code{list()}}
       List of parameters passed to \code{BatchJobs::makeRegistry()}. When
       present, user-supplied arguments \code{seed} and \code{work.dir}
       to \code{BatchJobsParam} are ignored.

   \item{conf.pars}{\code{list()}}
       List of parameters passed to \code{BatchJobs::setConfig()}. When
       present, user-supplied arguments \code{conffile},
       \code{cluster.functions} to \code{BatchJobsParam} are ignored.

   \item{submit.pars}{\code{list()}}
       List of parameters passed to \code{BatchJobs::submitJobs}. When
       present, user-supplied argument \code{resources} to
       \code{BatchJobsParam} is ignored. \code{submitJobs} parameters
       \code{reg}, \code{id} cannot be set.
   
    \item{\dots}{Addition arguments, currently not handled.}
}

\section{BatchJobsParam constructor}{

    Return an object with specified values. The object may be saved to
    disk or reused within a session.
}

\section{Methods}{

    The following generics are implemented and perform as documented on
    the corresponding help page: \code{\link{bpworkers}},
    \code{\link{bpnworkers}}, \code{\link{bpstart}},
    \code{\link{bpstop}}, \code{\link{bpisup}}, \code{\link{bpbackend}},
    \code{\link{bpbackend<-}}

}

\author{Michel Lang, \url{mailto:michellang@gmail.com}}

\seealso{

    \code{getClass("BiocParallelParam")} for additional parameter classes.

    \code{register} for registering parameter classes for use in parallel
    evaluation.

}

\examples{
p <- BatchJobsParam(progressbar=FALSE)
bplapply(1:10, sqrt, BPPARAM=p)

\dontrun{
## see vignette for additional explanation
funs <- makeClusterFunctionsSLURM("~/slurm.tmpl")
param <- BatchJobsParam(4, cluster.functions=funs)
register(param)
bplapply(1:10, function(i) sqrt)
}
}
