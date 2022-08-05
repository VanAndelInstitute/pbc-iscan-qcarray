#! /bin/bash
./resize.sh 20

# Uninstall AWS CLI version 1
sudo pip uninstall -y awscli
# Install AWS CLI version 2
wget "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"
unzip awscli-exe-linux-x86_64.zip
sudo ./aws/install
rm -rf aws awscli-exe-linux-x86_64.zip

# Install Cloud9 CLI
npm install -g c9

# Run the following commands to build,push,run the docker image
# docker build -t public.ecr.aws/pbc-iscan/qcarray-snakemake:latest .
# aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
# docker image push public.ecr.aws/pbc-iscan/qcarray-snakemake:latest
# docker run -it --rm -v $HOME/.aws/:/root/.aws/ public.ecr.aws/pbc-iscan/qcarray-snakemake:latest snakemake --profile awsbatch-profile
