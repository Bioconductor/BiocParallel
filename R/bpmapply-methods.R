setMethod(bpmapply, c("ANY", "missing"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
        resume=getOption("BiocParallel.resume", FALSE), BPPARAM)
{
    FUN <- match.fun(FUN)
    x <- registered()[[1]]
    bpmapply(FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
        USE.NAMES=USE.NAMES, resume=resume, BPPARAM=x)
})
