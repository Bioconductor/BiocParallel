setMethod(bpmapply, c("ANY", "missing"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
        BPRESUME=getOption("BiocParallel.BPRESUME", FALSE), BPPARAM)
{
    FUN <- match.fun(FUN)
    x <- registered()[[1]]
    bpmapply(FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
        USE.NAMES=USE.NAMES, BPRESUME=BPRESUME, BPPARAM=x)
})
