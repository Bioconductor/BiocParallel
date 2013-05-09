## compatibility; used in mclapply, pvec

children <- parallel:::children

closeStdout <- parallel:::closeStdout

isChild <- parallel:::isChild

mc.advance.stream <- parallel:::mc.advance.stream

mc.set.stream <- parallel:::mc.set.stream

mcexit <- parallel:::mcexit

mcfork <- parallel:::mcfork

mckill <- parallel:::mckill

processID <-parallel:::processID

readChild <- parallel:::readChild

selectChildren <- parallel:::selectChildren

sendMaster <- parallel:::sendMaster
    
.onLoad <-
    function(libname, pkgname)
{
    register(getOption("SnowParam", SnowParam(workers=detectCores())))
    register(getOption("MulticoreParam", MulticoreParam()))
}
