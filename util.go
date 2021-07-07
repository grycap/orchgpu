package main

import(
	"fmt"
	"strings"
	"os"
	"os/exec"
	"io/ioutil"
	"net/http"
	"io"

	"github.com/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/buger/jsonparser"
)

/* Auxiliary function to parse the rCUDA data received from SSGM */
func parse_rcuda_data(rcuda_data string) []string {
	rcuda_data_splits := strings.Split(rcuda_data, ";")
	for i, e := range rcuda_data_splits {
		rcuda_data_splits[i] = strings.Split(e, "=")[1]
	}
	return rcuda_data_splits
}

/* Auxiliary function to download an S3 object using a presigned URL  */
func download_s3_object(client bucket string, object string, cfg aws.Config, s3_object_path string) {
	fmt.Println("Generating presigned URL")
	client := s3.NewFromConfig(cfg)
	input := &s3.GetObjectInput{
		Bucket:	bucket,
		Key:	object,
	}
	psCLient := s3.NewPresignClient(client)
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
	_, err := io.Copy(out, http_resp.Body)
	if err != nil {
		panic("Error trying to save the S3 object")
	}
}

/* Auxiliary function to invoke the SCAR function */
//TODO check parameters with the main.go call
func invoke_scar(rcuda_data_splits []string, sqs_job_id string, script_path string, yaml_path string,
	ssgm_path string, scheduler_address string, scheduler_port string, sqs_job_body string, cfg aws.Config) {
	//If there is an error during the execution of this function, the scheduler job must be deallocated
	rcuda_job_id := strings.Split(rcuda_data_splits[1], "=")[1]
	dealloc_command := exec.Command(ssgm_path, "-S", scheduler_address, "-P", scheduler_port, "-dealloc", "-j", rcuda_job_id)
	defer dealloc_command.Run()

	//Write the rCUDA data in /tmp/job_id.sh
	rcuda_script_path := "/tmp/" + sqs_job_id + ".sh"
	rcuda_script, err := os.Create(rcuda_script_path)
	_, err := rcuda.script.WriteString("#!/bin/sh"+"\n")
	if err != nil {
		panic("Error writing file at /tmp/job_id.sh, " + err.Error())
	}
	for _, split := range rcuda_data_splits {
		_, err := rcuda_script.WriteString(split + "\n")
		if err != nil {
			panic("Error copying the rCUDA data into /tmp/job_id.sh, " + err.Error())
		}
	}

	//Download the S3 object using a presigned URL
	object_splits := strings.Split(object, ".")
	extension := ""
	if len(object_split) > 1 {
		extension = object_splits[1]
	}else{
		panic("Invalid file extension in the S3 object")
	}
	s3_object_path := "/tmp/" + sqs_job_id + extension
	bucket,_,_,_ := string(jsonparser.Get([]byte(*msgResult.Messages[0].Body), "Records", "[0]", "s3", "bucket", "name"))
	object,_,_,_ := string(jsonparser.Get([]byte(*msgResult.Messages[0].Body), "Records", "[0]", "s3", "object", "key"))
	if bucket == "" || object == "" {
		panic("Error parsing the SQS notification: bucket name or object key is empty")
	download_s3_object(bucket, object, cfg, s3_object_path)

	//Execute the SCAR function using the SCAR CLI
	//TODO change to scar put
	fmt.Println("Executing scar run...")
	err = exec.Command("scar", "run", "-f", yaml_path, "-s", filename).Run()
	if err != nil {
		panic("Error executing scar run: " + err.Error())
	}
	fmt.Println("scar run successfully executed")
	time.Sleep(30)//Might be needed if scar run is non-blocking

}

