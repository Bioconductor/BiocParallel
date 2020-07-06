#! /bin/sh

${RPROG:-R} --vanilla <<EOF > ${OUT:-/dev/null} 2>&1

loadNamespace("Rmpi")
loadNamespace("snow")

BiocParallel::bprunMPIworker()
EOF
