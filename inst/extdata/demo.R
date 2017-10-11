devtools::load_all()


image <- "ami-7d342906"

## RELEASE
image <-  "ami-9fe2fee4"
sg <- "sg-748dcd07"
subnet <- "subnet-d66a05ec"

access_key = "AKIAISE2U2VGKIRCWQTA"
secret_key = "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"

#access_key = strsplit(readLines("~/.aws/credentials"), " = ")[[2]][[2]]
#secret_key = strsplit(readLines("~/.aws/credentials"), " = ")[[3]][[2]]

workers = 1

aws <- AWSParam(
    workers,
    access_key,
    secret_key,
    "t2.micro",
    awsSubnet = subnet,
    awsSecurityGroup = sg,
    awsAmiId= image
)

aws

## Check if instance is up,
bpisup(aws)

## Start instance
bpstart(aws)
awsCluster()
ips <- .awsClusterIps(aws)

## Check is instance is up
bpisup(aws)

## Stop instance
bpstop(aws)


cl <- snow::makeSOCKcluster(
    ips,
    rshcmd = "ssh -i ~/.ssh/bioc-default.pem -v",
    user="ubuntu",
    rhome="/usr/local/lib/R",
    snowlib = "/home/ubuntu/R/x86_64-pc-linux-gnu-library/3.4",
    rscript = "/usr/local/bin/Rscript",
    outfile = "/home/ubuntu/snow.log",
    master = aws.ec2::my_ip()
)


