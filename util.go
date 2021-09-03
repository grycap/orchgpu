package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/buger/jsonparser"
)

/* Auxiliary function to download an S3 object using a presigned URL  */
func download_s3_object(bucket string, object string, client s3.Client, s3_object_path string) {
	fmt.Println("Generating presigned URL")
	input := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &object,
	}
	psCLient := s3.NewPresignClient(&client)
	resp, err := GetPresignedURL(context.TODO(), psCLient, input)
	if err != nil {
		panic("Error generating presigned URL: " + err.Error())
	}
	fmt.Println("Succesfully generated presigned URL")
	http_resp, err := http.Get(resp.URL)
	if err != nil {
		panic("Error trying to retreive object from S3")
	}
	defer http_resp.Body.Close()
	out, err := os.Create(s3_object_path)
	if err != nil {
		panic("Error creating the local S3 object file")
	}
	defer out.Close()
	_, err = io.Copy(out, http_resp.Body)
	if err != nil {
		panic("Error trying to save the S3 object")
	}
}

/* Auxiliary function to invoke the SCAR function */
func invoke_scar(rcuda_data_splits []string, sqs_job_id string, intermediate_bucket string, output_bucket string, ssgm_path string,
	scheduler_address string, scheduler_port string, sqs_job_body string, result_bucket_wait *int, cfg aws.Config) {
	//If there is an error during the execution of this function, the scheduler job must be deallocated
	rcuda_job_id := rcuda_data_splits[1]
	dealloc_command := exec.Command(ssgm_path, "-S", scheduler_address, "-P", scheduler_port, "-dealloc", "-j", rcuda_job_id)
	defer dealloc_command.Run()

	//Write the rCUDA data in /tmp/job_id.txt
	rcuda_txt_path := "/tmp/" + sqs_job_id + ".txt"
	rcuda_script, err := os.Create(rcuda_txt_path)
	_, err = rcuda_script.WriteString(rcuda_data_splits[2] + "\n")
	if err != nil {
		panic("Error copying the rCUDA data into /tmp/job_id.txt, " + err.Error())
	}
	_, err = rcuda_script.WriteString(rcuda_data_splits[3] + "\n")
	if err != nil {
		panic("Error copying the rCUDA data into /tmp/job_id.txt, " + err.Error())
	}

	//Parse the S3 SQS notification message to get the bucket name and object key
	bucket, err := jsonparser.GetString([]byte(sqs_job_body), "Records", "[0]", "s3", "bucket", "name")
	if err != nil {
		panic("Error parsing the S3 SQS notifcation to get the bucket name: " + err.Error())
	}
	object, err := jsonparser.GetString([]byte(sqs_job_body), "Records", "[0]", "s3", "object", "key")
	if err != nil {
		panic("Error parsing the S3 SQS notifcation to get the object key: " + err.Error())
	}
	if string(bucket) == "" || string(object) == "" {
		panic("Error parsing the SQS notification: bucket name or object key is empty")
	}

	//Define the S3 object file path in the local storage
	object_splits := strings.Split(object, ".")
	extension := ""
	if len(object_splits) > 1 {
		extension = "." + object_splits[1]
	} else {
		panic("Invalid file extension in the S3 object")
	}
	s3_object_path := "/tmp/" + sqs_job_id + extension

	//Download the S3 object using a presigned URL
	client := s3.NewFromConfig(cfg)
	download_s3_object(string(bucket), string(object), *client, s3_object_path)

	//Make a TAR file with the rcuda script and the s3 object
	compress_command := exec.Command("tar", "-czf", "/tmp/"+sqs_job_id+".tar.gz", "/tmp/"+sqs_job_id+".txt", "/tmp/"+sqs_job_id+extension)
	err = compress_command.Run()
	if err != nil {
		panic("Error making the TAR file: " + err.Error())
	}

	//Execute the SCAR function by uploading the tar file to the intermediate S3 bucket
	fmt.Println("Executing scar put" + " -b " + intermediate_bucket + " -p " + "/tmp/" + sqs_job_id + ".tar.gz")
	err = exec.Command("scar", "put", "-b", intermediate_bucket, "-p", "/tmp/"+sqs_job_id+".tar.gz").Run()
	if err != nil {
		panic("Error executing scar put: " + err.Error())
	}
	fmt.Println("scar put successfully executed")

	//scar-put is non-blocking, so the program needs to check that the result is in the output bucket before deallocating the job
	output_bucket_splits := strings.Split(output_bucket, "/")
	output_bucket_path := output_bucket_splits[0]
	output_bucket_dir := output_bucket_splits[1]
	input := &s3.ListObjectsV2Input{
		Bucket: &output_bucket_path,
	}
	for {
		fmt.Println("Polling " + output_bucket_path + " for the output file in path: " + output_bucket_dir + "/" + sqs_job_id + ".png")
		objects, err := GetObjects(context.TODO(), client, input)
		if err != nil {
			panic("Error polling the output S3 bucket" + err.Error())
		}
		found_result := false
		for _, object := range objects.Contents {
			if strings.Contains(*object.Key, output_bucket_dir+"/"+sqs_job_id+".png") {
				found_result = true
				break
			}
		}
		if found_result {
			fmt.Println("Result found in the output S3 bucket. Exiting goroutine")
			break //After breaking out, the deferred deallocation will happen
		} else {
			fmt.Println("Result not found in the output S3 bucket. Trying again in a few seconds...")
			time.Sleep(time.Duration(*result_bucket_wait) * time.Second)
		}
	}
}
