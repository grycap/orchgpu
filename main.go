package main

import (
	"context"
	"flag"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)


func main() {
	//Define CLI flags
	queue_name := flag.String("q", "", "Queue name")
	//scar_endpoint_url := flag.String("e", "", "Url del endpoint de SCAR")
	scheduler_address := flag.String("S", "", "Scheduler IP address")
	scheduler_port := flag.String("P", "", "Scheduler port")
	gpu_number := flag.String("g", "1", "Number of GPUs to allocate")
	visibility_timeout := flag.Int("v", 300, "SQS Visibility timeout")
	wait_time_seconds := flag.Int("w", 20, "SQS Wait time seconds")
	scheduler_allocation_timeout := flag.Int("s", 30, "Scheduler allocation timeout")
	//script_path := flag.String("p", "", "Script path")
	//yaml_path := flag.String("y", "", "YAML path")
	ssgm_path := flag.String("m", "", "SSGM executable path")
        intermediate_bucket := flag.String("i", "", "Intermediate S3 bucket")

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

	//Get the SQS queue URL
	urlResult, err := GetQueueURL(context.TODO(), client, qUInput)
	if err != nil {
		fmt.Println("Error trying to obtain the URL for queue " + *queue_name + ":")
		fmt.Println(err)
		return
	}
	queueURL := urlResult.QueueUrl
	fmt.Println("URL for queue " + *queue_name + " was successfully obtained")

	//In an infinite loop...
	for {
		//Get a message from the queue
		fmt.Println("Trying to pull a message from the queue")
		gMInput := &sqs.ReceiveMessageInput{
			MessageAttributeNames: []string{
				string(types.QueueAttributeNameAll),
			},
			QueueUrl:            queueURL,
			MaxNumberOfMessages: 1,
			VisibilityTimeout:   int32(*visibility_timeout),
			WaitTimeSeconds:     int32(*wait_time_seconds),
		}
		sqs_jobs, err := GetMessages(context.TODO(), client, gMInput)
		if err != nil {
			fmt.Println("Error trying to pull messsage from queue:")
			fmt.Println(err)
			return
		}
		if len(sqs_jobs.Messages) == 0 {
			fmt.Println("The SQS queue is empty. Waiting a few seconds before trying again...")
			time.Sleep(10)
			continue
		}
		sqs_job := sqs_jobs.Messages[0]
		sqs_job_id := sqs_job.MessageId
		sqs_job_receipt_handle := sqs_job.ReceiptHandle
		sqs_job_body := sqs_job.Body
		fmt.Println("Message " + *sqs_job_id + " has been pulled from the queue " + *queueURL)

		//Allocate scheduler resources using SSGM (blocking, waits for the answer until timeout expires)
		fmt.Println("Executing ssgm...")
		sched_ctx, cancel := context.WithTimeout(context.Background(),
			time.Duration(*scheduler_allocation_timeout)*time.Second)
		cmd := exec.CommandContext(sched_ctx, *ssgm_path, "-S", *scheduler_address, "-P",
			*scheduler_port, "-alloc", "-g", *gpu_number)
		out, err := cmd.Output()
		//If the ssgm command executes without error and rCUDA data is received before the timeout expires,
		//call the goroutine and delete the message from the SQS queue
		if sched_ctx.Err() != context.DeadlineExceeded {
			if err != nil {
				fmt.Println("The ssgm command has encountered an error: " + err.Error())
			} else {
				//Clean and parse the received data
				rcuda_data := strings.TrimSpace(string(out))
				rcuda_data_splits := strings.Split(rcuda_data, ";")
				//Last split is not needed. Operation is safe because len is never 0 in this case
				rcuda_data_splits = rcuda_data_splits[:len(rcuda_data_splits)-1]
				ssgm_error_value := strings.Split(rcuda_data_splits[0], "=")[1]
				if ssgm_error_value == string(1) {
					fmt.Println("SSGM has return SSGM_ERROR=1")
				} else {
					fmt.Println("SSGM has received rCUDA data containing: " + rcuda_data)
					//Invoke the SCAR function using the invoke_scar auxiliary function
					go invoke_scar(rcuda_data_splits, *sqs_job_id, *script_path, *intermediate_bucket,
						*ssgm_path, *scheduler_address, *scheduler_port, *sqs_job_body, cfg)
					//Delete the message from the queue
					fmt.Println("Deleting message from queue...")
					dMInput := &sqs.DeleteMessageInput{
						QueueUrl:      queueURL,
						ReceiptHandle: sqs_job_receipt_handle,
					}
					_, err := RemoveMessage(context.TODO(), client, dMInput)
					if err != nil {
						fmt.Println("An error happened while deleting the message:")
						fmt.Println(err)
						return
					}
					fmt.Println("Message " + *sqs_job_id + " was successfully deleted" + *queueURL)
				}
			}
		} else {
			fmt.Println("SSGM timeout expired, assuming the scheduler is busy")
		}
		cancel() //Cancel the sched_ctx context
	}
}
