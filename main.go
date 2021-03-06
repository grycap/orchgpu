package main

import (
	"context"
	"flag"
	"fmt"
	"os/exec"
	"strings"
	"time"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)


func main() {
	//Define CLI flags
	queue_name := flag.String("q", "", "Queue name")
	scheduler_address := flag.String("S", "", "Scheduler IP address")
	scheduler_port := flag.String("P", "", "Scheduler port")
	gpu_number := flag.String("g", "1", "Number of GPUs to allocate")
	visibility_timeout := flag.Int("v", 30, "SQS Visibility timeout")
	wait_time_seconds := flag.Int("w", 20, "SQS Wait time seconds")
	scheduler_allocation_timeout := flag.Int("s", 30, "Scheduler allocation timeout")
	sqs_empty_wait := flag.Int("u", 10, "Empty SQS wait time before retrying")
	result_bucket_wait := flag.Int("b", 60, "Wait time when polling the output S3 bucket for the result")
	ssgm_path := flag.String("m", "", "SSGM executable path")
	intermediate_bucket := flag.String("i", "", "Intermediate S3 bucket and directory")
	output_bucket := flag.String("o", "", "Output S3 bucket and directory")

	flag.Parse()

	//We could check flag values and print a help message here

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic("Config error: " + err.Error())
	}
	client := sqs.NewFromConfig(cfg)

	qUInput := &sqs.GetQueueUrlInput{
		QueueName: queue_name,
	}


	//Print start time for convenience
	fmt.Println("[main] Execution start time ", time.Now())

	//Get the SQS queue URL
	urlResult, err := GetQueueURL(context.TODO(), client, qUInput)
	if err != nil {
		fmt.Println("[main] Error trying to obtain the URL for queue " + *queue_name + ":")
		fmt.Println(err)
		return
	}
	queueURL := urlResult.QueueUrl
	fmt.Println("[main] URL for queue " + *queue_name + " was successfully obtained")

	//In an infinite loop...
	for {
		//Get a message from the queue
		fmt.Println("[main] Pulling a message from the queue...")
		gMInput := &sqs.ReceiveMessageInput{
			MessageAttributeNames: []string{
				string(types.QueueAttributeNameAll),
			},
			QueueUrl:            queueURL,
			MaxNumberOfMessages: 1,
			VisibilityTimeout:   int32(*visibility_timeout),
			WaitTimeSeconds:     int32(*wait_time_seconds),
		}
		sqs_jobs, err := GetMessages(context.TODO(), client, gMInput) //Blocking
		if err != nil {
			fmt.Println("[main] Error trying to pull messsage from queue:")
			fmt.Println(err)
			return
		}
		if len(sqs_jobs.Messages) == 0 {
			fmt.Println("[main] The SQS queue is empty. Waiting a few seconds before trying again...")
			time.Sleep(time.Duration(*sqs_empty_wait) * time.Second)
			continue
		}
		sqs_job := sqs_jobs.Messages[0]
		sqs_job_id := sqs_job.MessageId
		sqs_job_receipt_handle := sqs_job.ReceiptHandle
		sqs_job_body := sqs_job.Body
		fmt.Println("[main] Message " + *sqs_job_id + " has been pulled from the queue " + *queue_name)

		//Allocate scheduler resources using SSGM
		//Assumes the scheduler is working in non-blocking mode and returns the following error codes:
		//SSGM_ERROR=0 --> Resources correctly allocated
		//SSGM_ERROR=1 --> Request error
		//SSGM_ERROR=2 --> Resources are busy

		//These two variables will be used lated
		var rcuda_data string
		var rcuda_data_splits []string

		//Try to allocate a scheduler job
		for {
			fmt.Println("[main] Executing ssgm...")
			cmd := exec.Command(*ssgm_path, "-S", *scheduler_address, "-P",
				*scheduler_port, "-alloc", "-g", *gpu_number)
			out, err := cmd.Output()
			if err != nil {
				fmt.Println("[main] The ssgm command execution has encountered an error: " + err.Error())
			}
			//Clean and parse the received data
			rcuda_data = strings.TrimSpace(string(out))
			rcuda_data_splits = strings.Split(rcuda_data, ";")
			//Last split is not needed. Operation is safe because len is never 0 in this case
			rcuda_data_splits = rcuda_data_splits[:len(rcuda_data_splits)-1]
			//Extract the values of the rCUDA variables
			for i, e := range rcuda_data_splits {
				rcuda_data_splits[i] = strings.Split(e, "=")[1]
			}
			//Break out of the loop if SSGM_ERROR!=2
			if rcuda_data_splits[0] != strconv.Itoa(2) {
				break
			}
			//If not, wait a few seconds and try again
			fmt.Println("[main] Received SSGM_ERROR=2. Waiting a few seconds before trying again")
			time.Sleep(time.Duration(*scheduler_allocation_timeout) * time.Second)
		}
		//If the ssgm command executes with no error,
		//call the goroutine and delete the message from the SQS queue
		//Check the SSGM error value
		if rcuda_data_splits[0] == string(1) {
			fmt.Println("[main] SSGM has return SSGM_ERROR=1")
		} else {
			fmt.Println("[main] SSGM has received rCUDA data containing: ", rcuda_data)
			//Invoke the SCAR function using the invoke_scar auxiliary function
			go invoke_scar(rcuda_data_splits, *sqs_job_id, *intermediate_bucket, *output_bucket, *ssgm_path,
				*scheduler_address, *scheduler_port, *sqs_job_body, result_bucket_wait, cfg)
			//Delete the message from the queue
			fmt.Println("[main] Deleting message from queue...")
			dMInput := &sqs.DeleteMessageInput{
				QueueUrl:      queueURL,
				ReceiptHandle: sqs_job_receipt_handle,
			}
			_, err := RemoveMessage(context.TODO(), client, dMInput)
			if err != nil {
				fmt.Println("[main] An error happened while deleting the message:")
				fmt.Println(err)
				return
			}
			fmt.Println("[main] Message " + *sqs_job_id + " was successfully deleted from queue " + *queue_name)
		}
	}
}
