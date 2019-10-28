\name{ipcmutex}
\alias{ipclocked}
\alias{ipclock}
\alias{ipctrylock}
\alias{ipcunlock}
\alias{ipcid}
\alias{ipcremove}
\alias{ipcyield}
\alias{ipcvalue}
\alias{ipcreset}
\title{Inter-process locks and counters}

\description{

  Functions documented on this page enable locks and counters between
  processes on the \emph{same} computer.
 
  Use \code{ipcid()} to generate a unique mutex or counter identifier. A
  mutex or counter with the same \code{id}, including those in different
  processes, share the same state.

  \code{ipcremove()} removes external state associated with mutex or
  counters created with \code{id}.

  \code{ipclock()} blocks until the lock is
  obtained. \code{ipctrylock()} tries to obtain the lock, returning
  immediately if it is not available. \code{ipcunlock()} releases the
  lock. \code{ipclocked()} queries the lock to determine whether it is
  currently held.

  \code{ipcyield()} returns the current counter, and increments the
  value for subsequent calls. \code{ipcvalue()} returns the current
  counter without incrementing. \code{ipcreset()} sets the counter to
  \code{n}, such that the next call to \code{ipcyield()} or
  \code{ipcvalue()} returns \code{n}.

}

\usage{
## Utilities

ipcid(id)

ipcremove(id)

## Locks

ipclock(id)

ipctrylock(id)

ipcunlock(id)

ipclocked(id)

## Counters

ipcyield(id)

ipcvalue(id)

ipcreset(id, n = 1)

}
\arguments{

  \item{id}{character(1) identifier string for mutex or
    counter. \code{ipcid()} ensures that the identifier is universally
    unique.}

  \item{n}{integer(1) value from which \code{ipcyield()} will
    increment.}

}
\value{
  Locks:

  \code{ipclock()} creates a named lock, returning \code{TRUE}
  on success.

  \code{trylock()} returns \code{TRUE} if the lock is
  obtained, \code{FALSE} otherwise.

  \code{ipcunlock()} returns \code{TRUE} on success,
  \code{FALSE} (e.g., because there is nothing to unlock)
  otherwise.
  
  \code{ipclocked()} returns \code{TRUE} when \code{id} is locked, and
  \code{FALSE} otherwise.

  Counters:

  \code{ipcyield()} returns an integer(1) value representing the next
  number in sequence. The first value returned is 1.

  \code{ipcvalue()} returns the value to be returned by the next call to
  \code{ipcyield()}, without incrementing the counter. If the counter is
  no longer available, \code{ipcyield()} returns \code{NA}.

  \code{ipcreset()} returns \code{n}, invisibly.

  Utilities:

  \code{ipcid()} returns a character(1) unique identifier, with
  \code{id} (if not missing) prepended.

  \code{ipcremove()} returns (invisibly) \code{TRUE} if external
  resources were released or \code{FALSE} if not (e.g., because the
  resources has already been released).

}
\examples{
ipcid()

## Locks

id <- ipcid()

ipclock(id)
ipctrylock(id)
ipcunlock(id)
ipctrylock(id)
ipclocked(id)

ipcremove(id)

id <- ipcid()
system.time({
    ## about 1s, .2s for each process instead of .2s if no lock
    result <- bplapply(1:2, function(i, id) {
        BiocParallel::ipclock(id)
        Sys.sleep(.2)
        time <- Sys.time()
        BiocParallel::ipcunlock(id)
        time
    }, id)
})
ipcremove(id)
diff(sort(unlist(result, use.names=FALSE)))

## Counters

id <- ipcid()

ipcyield(id)
ipcyield(id)

ipcvalue(id)
ipcyield(id)

ipcreset(id, 10)
ipcvalue(id)
ipcyield(id)

ipcremove(id)

id <- ipcid()
result <- bplapply(1:2, function(i, id) {
    BiocParallel::ipcyield(id)
}, id)
ipcremove(id)
sort(unlist(result, use.names=FALSE))
}
