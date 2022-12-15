\name{bpiterate}
\alias{bpiterate}
\alias{bpiterate,ANY,ANY,missing-method}
\alias{bpiterate,ANY,ANY,SerialParam-method}
\alias{bpiterate,ANY,ANY,BiocParallelParam-method}
\alias{bpiterate,ANY,ANY,SnowParam-method}
\alias{bpiterate,ANY,ANY,DoparParam-method}
\alias{bpiterate,ANY,ANY,BatchtoolsParam-method}
\alias{bpiterateAlong}

\title{Parallel iteration over an indeterminate number of data chunks}

\description{

  \code{bpiterate} iterates over an indeterminate number of data chunks
  (e.g., records in a file). Each chunk is processed by parallel workers
  in an asynchronous fashion; as each worker finishes it receives a
  new chunk. Data are traversed a single time.

  When provided with a vector-like argument \code{ITER = X},
  \code{bpiterate} uses \code{bpiterateAlong} to produce the sequence of
  elements \code{X[[1]]}, \code{X[[2]]}, etc.

}

\usage{

bpiterate(
    ITER, FUN, ...,
    BPREDO = list(), BPPARAM=bpparam(), BPOPTIONS = bpoptions()
)

\S4method{bpiterate}{ANY,ANY,missing}(
    ITER, FUN, ...,
    BPREDO = list(), BPPARAM=bpparam(), BPOPTIONS = bpoptions())

\S4method{bpiterate}{ANY,ANY,BatchtoolsParam}(
    ITER, FUN, ..., REDUCE, init, reduce.in.order=FALSE,
    BPREDO = list(), BPPARAM=bpparam(), BPOPTIONS = bpoptions()
)

bpiterateAlong(X)
}

\arguments{

  \item{X}{
    An object (e.g., vector or list) with `length()` and `[[` methods
    available.
  }

  \item{ITER}{
    A function with no arguments that returns an object to process,
    generally a chunk of data from a file. When no objects
    are left (i.e., end of file) it should return NULL and continue to
    return NULL regardless of the number of times it is invoked after
    reaching the end of file. This function is run on the master.
  }
  \item{FUN}{
    A function to process the object returned by \code{ITER};
    run on parallel workers separate from the master. When
    BPPARAM is a MulticoreParam, FUN is `decorated` with additional
    arguments and therefore must have \dots in the signature.
  }
  \item{BPPARAM}{An optional \code{\link{BiocParallelParam}} instance
    determining the parallel back-end to be used during evaluation, or a
    \code{list} of \code{BiocParallelParam} instances, to be applied in
    sequence for nested calls to \code{bpiterate}.
  }

  \item{REDUCE}{Optional function that combines (reduces)
    output from \code{FUN}. As each worker returns, the data are
    combined with the \code{REDUCE} function.  \code{REDUCE} takes 2
    arguments; one is the current result and the other is the output of
    \code{FUN} from a worker that just finished.}

  \item{init}{Optional initial value for \code{REDUCE}; must be of the
    same type as the object returned from \code{FUN}.  When supplied,
    \code{reduce.in.order} is set to TRUE.}

  \item{reduce.in.order}{Logical. When TRUE, REDUCE is applied to the
    results from the workers in the same order the tasks were sent out.}

  \item{BPREDO}{An output from \code{bpiterate} with one or more failed
    elements. This argument cannot be used with \code{BatchtoolsParam}
  }

  \item{\dots}{Arguments to other methods, and named arguments for
    \code{FUN}.}

  \item{BPOPTIONS}{
    Additional options to control the behavior of the parallel
    evaluation, see \code{\link{bpoptions}}.
  }
}

\details{

  Supported for \code{SnowParam}, \code{MulticoreParam} and
  \code{BatchtoolsParam}.

  \code{bpiterate} iterates through an unknown number of data
  chunks, dispatching chunks to parallel workers as they
  become available. In contrast, other \code{bp*apply} functions
  such as \code{bplapply} or \code{bpmapply} require the number of
  data chunks to be specified ahead of time. This quality makes
  \code{bpiterate} useful for iterating through files of unknown length.

  \code{ITER} serves up chunks of data until the end of the file
  is reached at which point it returns NULL. Note that \code{ITER}
  should continue to return NULL reguardless of the number of times
  it is invoked after reaching the end of the file. \code{FUN}
  is applied to each object (data chunk) returned by \code{ITER}.

  \code{bpiterateAlong()} provides an interator for a vector or other
  object with \code{length()} and \code{[[} methods defined. It is used
  in place of the first argument \code{ITER=}

}

\value{

  By default, a \code{list} the same length as the number of chunks in
  \code{ITER()}. When \code{REDUCE} is used, the return is consistent
  with application of the reduction. When errors occur, the errors will
  be attached to the result as an attribute \code{errors}
}

\author{

  Valerie Obenchain \url{mailto:vobencha@fhcrc.org}.

}

\seealso{

  \itemize{
    \item \code{\link{bpvec}} for parallel, vectorized calculations.
    \item \code{\link{bplapply}} for parallel, lapply-like calculations.
    \item \code{\link{BiocParallelParam}} for details of \code{BPPARAM}.
    \item \code{\link{BatchtoolsParam}} for details of \code{BatchtoolsParam}.
  }

}

\examples{
## A simple iterator
ITER <- bpiterateAlong(1:10)
result <- bpiterate(ITER, sqrt)
## alteernatively, result <- bpiterate(1:10, sqrt)
unlist(result)

\dontrun{
if (require(Rsamtools) && require(RNAseqData.HNRNPC.bam.chr14) &&
    require(GenomicAlignments) && require(ShortRead)) {

  ## ----------------------------------------------------------------------
  ## Iterate through a BAM file
  ## ----------------------------------------------------------------------

  ## Select a single file and set 'yieldSize' in the BamFile object.
  fl <- RNAseqData.HNRNPC.bam.chr14_BAMFILES[[1]]
  bf <- BamFile(fl, yieldSize = 300000)

  ## bamIterator() is initialized with a BAM file and returns a function.
  ## The return function requires no arguments and iterates through the
  ## file returning data chunks the size of yieldSize.
  bamIterator <- function(bf) {
      done <- FALSE
      if (!isOpen( bf))
	  open(bf)

      function() {
	  if (done)
	      return(NULL)
	  yld <- readGAlignments(bf)
	  if (length(yld) == 0L) {
	      close(bf)
	      done <<- TRUE
	      NULL
	  } else yld
      }
  }

  ## FUN counts reads in a region of interest.
  roi <- GRanges("chr14", IRanges(seq(19e6, 107e6, by = 10e6), width = 10e6))
  counter <- function(reads, roi, ...) {
      countOverlaps(query = roi, subject = reads)
  }

  ## Initialize the iterator.
  ITER <- bamIterator(bf)

  ## The number of chunks returned by ITER() determines the result length.
  bpparam <- MulticoreParam(workers = 3)
  ## bpparam <- BatchtoolsParam(workers = 3), see ?BatchtoolsParam
  bpiterate(ITER, counter, roi = roi, BPPARAM = bpparam)

  ## Re-initialize the iterator and combine on the fly with REDUCE:
  ITER <- bamIterator(bf)
  bpparam <- MulticoreParam(workers = 3)
  bpiterate(ITER, counter, REDUCE = sum, roi = roi, BPPARAM = bpparam)

  ## ----------------------------------------------------------------------
  ## Iterate through a FASTA file
  ## ----------------------------------------------------------------------

  ## Set data chunk size with 'n' in the FastqStreamer object.
  sp <- SolexaPath(system.file('extdata', package = 'ShortRead'))
  fl <- file.path(analysisPath(sp), "s_1_sequence.txt")

  ## Create an iterator that returns data chunks the size of 'n'.
  fastqIterator <- function(fqs) {
      done <- FALSE
      if (!isOpen(fqs))
	  open(fqs)

      function() {
	  if (done)
	      return(NULL)
	  yld <- yield(fqs)
	  if (length(yld) == 0L) {
	      close(fqs)
	      done <<- TRUE
	      NULL
	  } else yld
      }
  }

  ## The process function summarizes the number of times each sequence occurs.
  summary <- function(reads, ...) {
       ShortRead::tables(reads, n = 0)$distribution
  }

  ## Create a param.
  bpparam <- SnowParam(workers = 2)

  ## Initialize the streamer and iterator.
  fqs <- FastqStreamer(fl, n = 100)
  ITER <- fastqIterator(fqs)
  bpiterate(ITER, summary, BPPARAM = bpparam)

  ## Results from the workers are combined on the fly when REDUCE is used.
  ## Collapsing the data in this way can substantially reduce memory
  ## requirements.
  fqs <- FastqStreamer(fl, n = 100)
  ITER <- fastqIterator(fqs)
  bpiterate(ITER, summary, REDUCE = merge, all = TRUE, BPPARAM = bpparam)

  }
}
}

\keyword{manip}
\keyword{methods}
