###############################
# bpstart
library("aws.ec2")

aws.signature::use_credentials(profile = "mtmorgan")
image <- "ami-7d342906"
sg <- "sg-748dcd07"
subnet <- "subnet-d66a05ec"

access_key = strsplit(readLines("~/.aws/credentials"), " = ")[[2]][[2]]
secret_key = strsplit(readLines("~/.aws/credentials"), " = ")[[3]][[2]]
a <- AWSParam(
    1, access_key, secret_key, "t2.micro",
    awsSubnet = subnet, awsSecurityGroup = sg, awsAmiId= image
)
bpstart(a)

xx <- describe_instances(awsInstance(a))
ip <- xx[[1]]$instancesSet[[1]]$ipAddress

snow::makeSOCKcluster(
    ip,
    user="ubuntu",
    rscript = "/usr/local/bin/Rscript",
    snowlib = "/home/ubuntu/R/x86_64-pc-linux-gnu-library/3.4",
    outfile = "/home/ubuntu/snow-out.log",
    master = "67.99.175.226",
    port = "11777"
)


image_release <- "ami-9fe2fee4"


image <- image_devel
describe_images(image)
s <- describe_subnets()
g <- describe_sgroups()
s
s[[1]]
s[[4]]
g[[40]]
image_types = c("t2.micro")
# sg <- "sg-748dcd07"
# subnet <- "subnet-d66a05ec"
sg_index <- g %>% sapply(., "[[", "groupId") %in% sg %>% which(.)
subnet_index <- s %>% sapply(., "[[", "subnetId") %in% subnet %>% which(.)
s[[4]]
i <- run_instances(image = image, 
                   type = "t2.micro", # <- you might want to change this
                   subnet = s[[4]], 
                   sgroup = g[[40]])

i <- run_instances(image = image, 
                   type = "t2.micro", # <- you might want to change this
                   subnet = "subnet-d66a05ec", 
                   sgroup = "sg-748dcd07")
image

## instance status
state <- instance_status(i)$item$instanceState$name[[1]]
state
if (state == "running") {
    print("yes")
}

# Bpstop
res <- terminate_instances(i)
###############################

## API

aws = AWSParam$new(aws_access_key,
                   aws_secret_key,
                   instance_type="t2.micro",
                   bioc_version="devel")

instance_type(aws)

bioc_version(aws)

aws$bpstart


