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
BatchJobsParam(workers, catch.errors = TRUE, cleanup = TRUE, 
    work.dir = getwd(), stop.on.error = FALSE, seed = NULL,
    resources = NULL, conffile = NULL, cluster.functions = NULL, 
    progressbar = TRUE, ...) 
}

\arguments{

    \item{workers}{\code{integer(1)}} Number of workers on with
        Multicore and SSH backend which defaults here to all workers
        available.  On managed HPC \code{workers} defaults to \code{NA}
        but can be set to control chunking of jobs.  See argument
        \code{n.chunks} in \code{\link[BBmisc]{chunk}} and
        \code{\link[BatchJobs]{submitJobs}} for more information.

    \item{catch.errors}{\code{logical(1)}} Flag to determine in
        apply-like functions (see e.g. \code{\link{bplapply}}) whether
        to quit with an error as soon as one application fails or
        encapsulation of function calls in \code{\link[base]{try}}
        blocks which triggers a resume mechanism (see
        \link{bpresume}). Defaults to \code{TRUE}.

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
        \code{\link[base]{getwd}}.

    \item{stop.on.error}{\code{logical(1)}} Stop all jobs as soon as one
        jobs fails (\code{stop.on.error == TRUE}) or wait for all jobs
        to terminate.  Default is \code{FALSE}.

    \item{seed}{\code{integer(1L)}} Set an initial seed for the RNG. See
        \code{\link[BatchJobs]{makeRegistry}} for more information.
        Default is \code{NULL} where a random seed is chosen upon
        initialization.

    \item{resources}{\code{list()}} List of job specific resources
        passed to \code{\link[BatchJobs]{submitJobs}}.  Default is
        \code{NULL} where the resources defined in the configuration are
        used.

    \item{conffile}{\code{character(1)}} URI to a custom BatchJobs
        configuration file used for execution.  Default is \code{NULL}
        which relies on BatchJobs to handle configuration files.

    \item{cluster.functions}{ClusterFunctions} Specify a specific
        cluster backend using on of the constructors provided by
        BatchJobs, see \link[BatchJobs]{ClusterFunctions}.  Default is
        \code{NULL} where the default cluster functions defined in the
        configuration are used.

    \item{progressbar}{logical(1)}
        Suppress the progress bar used in BatchJobs and be less verbose.
        Default is \code{FALSE}.

    \item{\dots}{Addition arguments, currently not handled.}
}

\section{BatchJobsParam constructor}{

    Return an object with specified values. The object may be saved to
    disk or reused within a session.
}

\section{Methods}{

    The following generics are implemented and perform as documented on
    the corresponding help page:
    \code{\link{bpworkers}}, \code{\link{bpstart}}, \code{\link{bpstop}},
    \code{\link{bpisup}}, \code{\link{bpbackend}},
    \code{\link{bpbackend<-}}

}

\author{Martin Morgan \url{mailto:mtmorgan@fhcrc.org}}

\seealso{

    \code{getClass("BiocParallelParam")} for additional parameter classes.

    \code{register} for registering parameter classes for use in parallel
    evaluation.

}

\examples{
p <- BatchJobsParam()
bplapply(1:10, sqrt, BPPARAM=p)

\dontrun{
register(BatchJobsParam(catch.errors = FALSE), default=TRUE)
}
}
