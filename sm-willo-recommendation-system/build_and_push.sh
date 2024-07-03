# Name of algo -> ECR
algorithm_name="sm-willo-recommendation-system"

#make serve executable
chmod +x REC_SYS/serve
account=$(aws sts get-caller-identity --query Account --output text)

echo ${account}

# create container full name
region=$(aws configure get region)
region=${region:-us-west-2}
fullname="${account}.dkr.ecr.${region}.amazonaws.com/${algorithm_name}:latest"

echo ${region}
echo ${fullname}

# If the repository doesn't exist in ECR, create it.
aws ecr describe-repositories --repository-names "${algorithm_name}" > /dev/null 2>&1
if [ $? -ne 0 ]
then
    aws ecr create-repository --repository-name "${algorithm_name}" > /dev/null
fi

# Get the login command from ECR and execute it directly
aws ecr get-login-password --region ${region} | docker login --username AWS --password-stdin ${fullname}

# Build the docker image locally with the image name and then push it to ECR
# with the full name.
docker build --platform=linux/amd64 -t ${algorithm_name} .
docker tag ${algorithm_name} ${fullname}
docker push ${fullname}