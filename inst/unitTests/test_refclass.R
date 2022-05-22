message("Testing refclass")

test_SnowParam_refclass <- function()
{
    p <- SnowParam(2)
    p2 <- p
    checkTrue(!bpisup(p))
    checkTrue(!bpisup(p2))
    bpstart(p)
    checkTrue(bpisup(p))
    checkTrue(bpisup(p2))
    bpstop(p)
    checkTrue(!bpisup(p))
    checkTrue(!bpisup(p2))
}
