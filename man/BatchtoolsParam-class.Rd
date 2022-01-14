\name{BatchtoolsParam-class}
\Rdversion{1.1}
\docType{class}

\alias{BatchtoolsParam-class}
\alias{BatchtoolsParam}
\alias{bpRNGseed,BatchtoolsParam-method}
\alias{bpRNGseed<-,BatchtoolsParam,numeric-method}
\alias{bpbackend,BatchtoolsParam-method}
\alias{bpisup,BatchtoolsParam-method}
\alias{bplapply,ANY,BatchtoolsParam-method}
\alias{bplogdir,BatchtoolsParam-method}
\alias{bplogdir<-,BatchtoolsParam,character-method}
\alias{bpschedule,BatchtoolsParam-method}
\alias{bpstart,BatchtoolsParam-method}
\alias{bpstop,BatchtoolsParam-method}
\alias{bpworkers,BatchtoolsParam-method}
\alias{show,BatchtoolsParam-method}
\alias{batchtoolsWorkers}
\alias{batchtoolsCluster}
\alias{batchtoolsTemplate}
\alias{batchtoolsRegistryargs}

\title{Enable parallelization on batch systems}

\description{

    This class is used to parameterize scheduler options on managed
    high-performance computing clusters using batchtools.

    \code{BatchtoolsParam()}: Construct a BatchtoolsParam-class object.

    \code{batchtoolsWorkers()}: Return the default number of workers for
    each backend.

    \code{batchtoolsTemplate()}: Return the default template for each
    backend.

    \code{batchtoolsCluster()}: Return the default cluster.

    \code{batchtoolsRegistryargs()}: Create a list of arguments to be
    used in batchtools' \code{makeRegistry}; see \code{registryargs}
    argument.

}

\usage{
BatchtoolsParam(
    workers = batchtoolsWorkers(cluster),
    cluster = batchtoolsCluster(),
    registryargs = batchtoolsRegistryargs(),
    saveregistry = FALSE,
    resources = list(),
    template = batchtoolsTemplate(cluster),
    stop.on.error = TRUE, progressbar = FALSE, RNGseed = NA_integer_,
    timeout = WORKER_TIMEOUT, exportglobals=TRUE,
    log = FALSE, logdir = NA_character_, resultdir=NA_character_,
    jobname = "BPJOB"
)
batchtoolsWorkers(cluster = batchtoolsCluster())
batchtoolsCluster(cluster)
batchtoolsTemplate(cluster)
batchtoolsRegistryargs(...)
}

\arguments{

    \item{workers}{\code{integer(1)}} Number of workers to divide tasks
	(e.g., elements in the first argument of \code{bplapply})
	between. On 'multicore' and 'socket' backends, this defaults to
	\code{multicoreWorkers()} and \code{snowWorkers()}.  On managed
	(e.g., slurm, SGE) clusters \code{workers} has no default,
	meaning that the number of workers needs to be provided by the
	user.

    \item{cluster}{\code{character(1)}} Cluster type being used as the
	backend by \code{BatchtoolsParam}. The available options are
	"socket", "multicore", "interactive", "sge", "slurm", "lsf",
	"torque" and "openlava". The cluster type if available on the
	machine registers as the backend. Cluster types which need
	a \code{template} are "sge", "slurm", "lsf", "openlava", and
	"torque". If the template is not given then a default is
	selected from the \code{batchtools} package.

    \item{registryargs}{\code{list()}} Arguments given to the registry
        created by \code{BatchtoolsParam} to configure the registry and
        where it's being stored. The \code{registryargs} can be
        specified by the function \code{batchtoolsRegistryargs()} which
        takes the arguments \code{file.dir}, \code{work.dir},
        \code{packages}, \code{namespaces}, \code{source}, \code{load},
        \code{make.default}. It's useful to configure these option,
        especially the \code{file.dir} to a location which is accessible
        to all the nodes on your job scheduler i.e master and
        workers. \code{file.dir} uses a default setting to make a
        registry in your working directory.

    \item{saveregistry}{\code{logical(1)}} Option given to store the
        entire registry for the job(s). This functionality should only
        be used when debugging. The storage of the entire registry can
        be time and space expensive on disk. The registry will be saved
        in the directory specified by \code{file.dir} in
        \code{registryargs}; the default locatoin is the current working
        directory. The saved registry directories will have suffix "-1",
        "-2" and so on, for each time the \code{BatchtoolsParam} is
        used.

    \item{resources}{\code{named list()}} Arguments passed to the
        \code{resources} argument of \code{batchtools::submitJobs}
        during evaluation of \code{bplapply} and similar
        functions. These name-value pairs are used for substitution
        into the template file.

    \item{template}{\code{character(1)}} Path to a template for the
        backend in \code{BatchtoolsParam}. It is possible to check which
	template is being used by the object using the getter
	\code{bpbackend(BatchtoolsParam())}. The template needs to be
	written specific to each backend. Please check the list of available
	templates in the \code{batchtools} package.

    \item{stop.on.error}{\code{logical(1)}} Stop all jobs as soon as one
	jobs fails (\code{stop.on.error == TRUE}) or wait for all jobs
	to terminate.  Default is \code{TRUE}.

    \item{progressbar}{\code{logical(1)}} Suppress the progress bar used
        in BatchtoolsParam and be less verbose.  Default is
        \code{FALSE}.

    \item{RNGseed}{\code{integer(1)}} Set an initial seed for the RNG.
	Default is \code{NULL} where a random seed is chosen upon
	initialization.

    \item{timeout}{\code{list()}} Time (in seconds) allowed for worker
        to complete a task.  If the computation exceeds \code{timeout}
        an error is thrown with message 'reached elapsed time limit'.

    \item{exportglobals}{\code{logical(1)}} Export
        \code{base::options()} from manager to workers? Default \code{TRUE}.

    \item{log}{\code{logical(1)}} Option given to save the logs which
        are produced by the jobs. If \code{log=TRUE} then the \code{logdir}
	option must be specified.

    \item{logdir}{\code{character(1)}} Path to location where logs are
    stored. The argument \code{log=TRUE} is required before using the
    logdir option.

    \item{resultdir}{\code{logical(1)}} Path where results are stored.

    \item{jobname}{\code{character(1)}} Job name that is prepended
    to the output log and result files. Default is "BPJOB".

    \item{\dots}{name-value pairs}
    Names and values correspond to arguments from batchtools
    \code{\link[batchtools]{makeRegistry}}.

}

\section{BatchtoolsParam constructor}{

    Return an object with specified values. The object may be saved to
    disk or reused within a session.
}

\section{Methods}{

    The following generics are implemented and perform as documented on
    the corresponding help page: \code{\link{bpworkers}},
    \code{\link{bpnworkers}}, \code{\link{bpstart}},
    \code{\link{bpstop}}, \code{\link{bpisup}}, \code{\link{bpbackend}}.

    \code{\link{bplapply}} handles arguments \code{X} of classes derived
    from \code{S4Vectors::List} specially, coercing to \code{list}.
}

\author{Nitesh Turaga, \url{mailto:nitesh.turaga@roswellpark.org}}

\seealso{

    \code{getClass("BiocParallelParam")} for additional parameter classes.

    \code{register} for registering parameter classes for use in parallel
    evaluation.

    The batchtools package.
}

\examples{
## Pi approximation
piApprox = function(n) {
    nums = matrix(runif(2 * n), ncol = 2)
    d = sqrt(nums[, 1]^2 + nums[, 2]^2)
    4 * mean(d <= 1)
}

piApprox(1000)

## Calculate piApprox 10 times
param <- BatchtoolsParam()
result <- bplapply(rep(10e5, 10), piApprox, BPPARAM=param)

\dontrun{
## see vignette for additional explanation
library(BiocParallel)
param = BatchtoolsParam(workers=5,
                        cluster="sge",
                        template="script/test-sge-template.tmpl")
## Run parallel job
result = bplapply(rep(10e5, 100), piApprox, BPPARAM=param)

## bpmapply
param = BatchtoolsParam()
result = bpmapply(fun, x = 1:3, y = 1:3, MoreArgs = list(z = 1),
                   SIMPLIFY = TRUE, BPPARAM = param)

## bpvec
param = BatchtoolsParam(workers=2)
result = bpvec(1:10, seq_along, BPPARAM=param)

## bpvectorize
param = BatchtoolsParam(workers=2)
## this returns a function
bpseq_along = bpvectorize(seq_along, BPPARAM=param)
result = bpseq_along(1:10)

## bpiterate
ITER <- function(n=5) {
        i <- 0L
        function() {
            i <<- i + 1L
            if (i > n)
                return(NULL)
        rep(i, n)
        }
    }

param <- BatchtoolsParam()
res <- bpiterate(ITER=ITER(), FUN=function(x,y) sum(x) + y, y=10, BPPARAM=param)

## save logs
logdir <- tempfile()
dir.create(logdir)
param <- BatchtoolsParam(log=TRUE, logdir=logdir)
res <- bplapply(rep(10e5, 10), piApprox, BPPARAM=param)

## save registry (should be used only for debugging)
file.dir <- tempfile()
registryargs <- batchtoolsRegistryargs(file.dir = file.dir)
param <- BatchtoolsParam(saveregistry = TRUE, registryargs = registryargs)
res <- bplapply(rep(10e5, 10), piApprox, BPPARAM=param)
dir(dirname(file.dir), basename(file.dir))
}
}
