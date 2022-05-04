# pbc-iscan-qcarray - Analysis
This part of the workflow (QC) is implemented as a Jupyter notebook in R.

## bafRegress
The bafRegress code has been pared down to only the part that does the linear regression. The parts for Python/R marshalling are no longer needed.

Also, the population minor allele frequencies (popmaf) are calculated from the genotype call (.gtc) files directly, rather than generating .ped files and using plink.

## argyle
Code was added to initialize the `genotypes` object from the genotype call (.gtc) files directly instead of generating and using a GenomeStudio final report text file.

## (Re)building the custom Docker image
If creating the custom image from scratch, follow the [Bring your own custom SageMaker image tutorial](https://docs.aws.amazon.com/sagemaker/latest/dg/studio-byoi-create-sdk.html). The basis for the custome R kernel can be found here: [sagemaker-studio-custom-image-samples / examples / r-image /](https://github.com/aws-samples/sagemaker-studio-custom-image-samples/tree/main/examples/r-image)

Otherwise, build and crete a new version of the custom image:

```
source /opt/conda/bin/activate
sm-docker build . --compute-type BUILD_GENERAL1_LARGE

#Output:
#Image URI: <acct-id>.dkr.ecr.<region>.amazonaws.com/<image_name>

python3 -c "import boto3; boto3.client('sagemaker').create_image_version( \
BaseImage='<acct-id>.dkr.ecr.<region>.amazonaws.com/<image_name>', \
ImageName='custom-r')"
```
