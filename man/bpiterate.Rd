\name{bpiterate}
\alias{bpiterate}
\alias{bpiterate,ANY,ANY,missing-method}
\alias{bpiterate,ANY,ANY,BiocParallelParam-method}
\alias{bpiterate,ANY,ANY,MulticoreParam-method}
\alias{bpiterate,ANY,ANY,SerialParam-method}
\alias{bpiterate,ANY,ANY,SnowParam-method}
\alias{bpiterate,ANY,ANY,BatchJobsParam-method}
\alias{bpiterate,ANY,ANY,DoparParam-method}

\title{Parallel iteration over an indeterminate number of data chunks}

\description{
  \code{bpiterate} iterates over an indeterminate number of data chunks
  (e.g., records in a file). Each chunk is processed by parallel workers 
  in an asynchronous fashion; as each worker finishes it recieves a 
  new chunk. Data are traversed a single time.
}

\usage{
bpiterate(ITER, FUN, ..., BPPARAM=bpparam())

\S4method{bpiterate}{ANY,ANY,missing}(ITER, FUN, ..., BPPARAM=bpparam())

\S4method{bpiterate}{ANY,ANY,BiocParallelParam}(ITER, FUN, ..., BPPARAM=bpparam())
}

\arguments{
  \item{ITER}{
    A function with no arguments that returns an object to process,
    generally a chunk of data from a file. When no objects 
    are left (i.e., end of file) it should return NULL and continue to
    return NULL reguardless of the number of times it is invoked after
    reaching the end of file. This function is run on the master.}

  \item{FUN}{
    A function to process the object returned by \code{ITER};
    run on parallel workers separate from the master. When
    BPPARAM is a MulticoreParam, FUN is `decorated` with additional
    arguments and therefore must have \dots in the signature.}

  \item{BPPARAM}{An optional \code{\link{BiocParallelParam}} instance
    determining the parallel back-end to be used during evaluation, or a
    \code{list} of \code{BiocParallelParam} instances, to be applied in
    sequence for nested calls to \code{bpiterate}.

    Currently only MulticoreParam is supported for \code{bpiterate}.}

  \item{\dots}{Arguments to other methods, specifically named 
    arguments for \code{FUN}, or \code{REDUCE} or \code{init}.

    \itemize{
      \item{\code{REDUCE}: } Optional function that combines
        (reduces) output from \code{FUN}. As each worker returns,
        the data are combined with the \code{REDUCE} function.
        \code{REDUCE} takes 2 arguments; one is the current
        result and the other is the output of \code{FUN} from
        a worker that just finished. Currently not supported
        on Windows.

      \item{\code{init}: } Optional initial value for \code{REDUCE};
        must be of the same type as the object returned from \code{FUN}.
        When supplied, \code{reduce.in.order} is set to TRUE.

      \item{\code{reduce.in.order}: } Logical. When TRUE, REDUCE
        is applied to the results from the workers in the same order 
        the tasks were sent out.
    }
  }
}

\details{
  Currently only MulticoreParam and SerialParam are supported.

  \code{bpiterate} iterates through an unknown number of data
  chunks, dispatching chunks to parallel workers as they
  become available. In contrast, other \code{bp*apply} functions
  such as code{bplapply} or \code{bpmapply} require the number of 
  data chunks to be specified ahead of time. This quality makes 
  \code{bpiterate} useful for iterating through files of unknown length.

  \code{ITER} serves up chunks of data until the end of the file
  is reached at which point it returns NULL. Note that \code{ITER}
  should continue to return NULL reguardless of the number of times
  it is invoked after reaching the end of the file. \code{FUN}
  is applied to each object (data chunk) returned by \code{ITER}.
}

\value{A \code{list} of output type specified by \code{FUN}. When
  \code{REDUCE} is supplied the list is of length 1.
}

\author{
  Valerie Obenchain \url{mailto:vobencha@fhcrc.org}.

  The multi-core implementation is a modification of the \code{sclapply} 
  in \code{HTSeqGeni} by Gregorie Pau.
}

\seealso{
  \itemize{
    \item \code{\link{bpvec}} for parallel, vectorized calculations.
    \item \code{\link{bplapply}} for parallel, lapply-like calculations.
    \item \code{\link{BiocParallelParam}} for details of \code{BPPARAM}.
  } 
}

\examples{

if (all(require(Rsamtools) && 
        require(RNAseqData.HNRNPC.bam.chr14) &&
        require(GenomicAlignments) &&
        require(ShortRead) &&
        .Platform$OS.type != "windows")) {

  ## ----------------------------------------------------------------------
  ## Iterating through a BAM file
  ## ----------------------------------------------------------------------
 
  ## Select a single file and set 'yieldSize' in the BamFile object.
  fl <- RNAseqData.HNRNPC.bam.chr14_BAMFILES[[1]]
  bf <- BamFile(fl, yieldSize = 300000) 
 
  ## bamIterator() is initalized with a BAM file and returns a function.
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
 
  ## Initalize the iterator.
  ITER <- bamIterator(bf)
 
  ## Create a FUN that counts reads in a region of interest.
  roi <- GRanges("chr14", IRanges(seq(19e6, 107e6, by = 10e6), width = 10e6))
  counter <- function(reads, roi, ...) {
      countOverlaps(query = roi, subject = reads)
  }
  ## Create a MulticoreParam and call bpiterate().
  bpparam <- MulticoreParam(workers = 2) 
  res <- bpiterate(ITER, counter, BPPARAM = bpparam, roi = roi)
 
  ## The result length is the same as the number of data chunks.
  length(res)
  colSums(do.call(rbind, res))
 
  ## ----------------------------------------------------------------------
  ## Iterating through a FASTA file
  ## ----------------------------------------------------------------------
 
  ## Set data chunk size with 'n' in the FastqStreamer object.
  sp <- SolexaPath(system.file('extdata', package = 'ShortRead'))
  fl <- file.path(analysisPath(sp), "s_1_sequence.txt")
  fqs <- FastqStreamer(fl, n = 100)
 
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
 
  ## Initialize the iterator.
  ITER <- fastqIterator(fqs)
 
  ## The processor summarizes the number of times each sequence occurs.
  summary <- function(reads, ...) {
       tables(reads, n = 0)$distribution
  }
 
  bpparam <- MulticoreParam(workers = 2) 
  bpiterate(ITER, summary, BPPARAM = bpparam)
 
  ## Results from the workers are combined on the fly when a 
  ## REDUCE function is provided. Collapsing the data in this 
  ## way can substantially reduce memory requirements.
  fqs <- FastqStreamer(fl, n = 100)
  ITER <- fastqIterator(fqs)
  bpiterate(ITER, summary, BPPARAM = bpparam, REDUCE = merge, all = TRUE)
 
  ## ----------------------------------------------------------------------
  ## Multiple files
  ## ----------------------------------------------------------------------
  ## Currently bpiterate() is only implemented for the multi-core
  ## environment (i.e., MulticoreParam). A single machine may not
  ## have enough memory to process multiple files. In this case the
  ## files can be distributed over a snow cluster with bplapply() 
  ## then processed with bpiterate() on each cluster node.
 
  ## Select a subset of files, create a BamFileList.
  fls <- RNAseqData.HNRNPC.bam.chr14_BAMFILES[1:3]
  bfl <- BamFileList(fls, yieldSize = 200000) 
 
  ## Cluster size is defined by the number of files.
  snowp <- SnowParam(workers = length(bfl))
 
  ## Currently bpiterate() is only supported in the multi-core environment
  ## and must use MulticoreParam.
  myFUN <- function(file, bamIterator, counter, ...) {
    library(BiocParallel)  ## for bpiterate
    library(Rsamtools)     ## for Bam file manipulation
    library(GenomicAlignments) ## for readGAlignments
    ITER <- bamIterator(file)
    bpiterate(ITER, counter, BPPARAM = MulticoreParam(workers=2), roi = roi)
  }
 
  ## Distribute the files across the snow cluster workers with bplapply().
  ## Each cluster node acts as a master and spawns 2 children. 
  res <- bplapply(bfl, myFUN, BPPARM = snowp, bamIterator = bamIterator,
                  counter = counter, roi = roi) 

  ## The result is of length 3 (# of files).
  ## > length(res)
  ## [1] 3

 
  ## Each list element is of length 5 (# of iterations of size 'yieldSize').
  ## >  elementLengths(res)
  ## ERR127306 ERR127307 ERR127308 
  ##         5         5         5 
}
}

\keyword{manip}
\keyword{methods}
