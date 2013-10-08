\name{bpresume}
\alias{bpresume}
\alias{bplasterror}


\title{Resume computation with partial results}

\description{
   Resume partial successful calls to  \code{\link{bplapply}} or \code{\link{bpmapply}}
}

\usage{
    bplasterror()
    bpresume(expr)
}

\arguments{
    \item{expr}{expression}
        A expression which calls either \code{bplapply} or \code{bpmapply}.
}

\section{Usage}{
    The resume mechanism is triggered if the argument \code{catch.errors} of
    the \code{\link{BiocParallelParam}} class is set to \code{TRUE}.
    The methods \code{\link{bplapply}} and \code{\link{bpmapply}} then store the current
    state of computation.
    Recalling the call directory with argument \code{resume} set to \code{TRUE}
    will then only compute the missing parts of the previous call and merge the
    results.

    Alternatively, if the call to \code{\link{bplapply}} and \code{\link{bpmapply}} 
    is inside a function not accessible directly by the user, the last call can
    be embedded into \code{bpresume} which sets an option accordingly to enable
    the resume feature down in the call stack.

    The function \code{bplasterror} returns a \code{LastError} object containing
    the partial results and errors to investigate.

    Note that nested calls of the apply functions can cause troubles in some scenarios.
}

\author{Martin Morgan \url{mailto:mtmorgan@fhcrc.org}}

