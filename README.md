# orchgpu

A piece of Go code to enable serverless GPU-based computing using the rCUDA scheduler.

#### File overview
* **main.go** Parses input arguments and executes an infinite loop that gets a message from an AWS SQS queue, allocates GPU resources and orders the async invocation of a remote SCAR function.
* **util.go** Contains functions to parse the rCUDA data received from the scheduler and invoke the SCAR function.
* **api.go** Contains some helper interfaces and methods to communicate with AWS SQS and S3, as shown in the AWS SDK for Go v2 documentation.
