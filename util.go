package main

import(
	"fmt"
	"strings"
	"os"
	"os/exec"
	"io/ioutil"
)

/* Parses the rCUDA data received from SSGM */
func parse_rcuda_data(rcuda_data string) []string {
	rcuda_data_splits := strings.Split(rcuda_data, ";")
	for i, e := range rcuda_data_splits {
		rcuda_data_splits[i] = strings.Split(e, "=")[1]
	}
	return rcuda_data_splits
}

/* Invokes the remote SCAR function using a copy of its script with the rCUDA data added to it */
func invoke_scar(rcuda_data_splits []string, sqs_job_id string, script_path string,
	yaml_path string, ssgm_path string, scheduler_address string, scheduler_port string) {
	//If there is an error during the execution of this function, the scheduler job must be deallocated
	rcuda_job_id := strings.Split(rcuda_data_splits[1], "=")[1]
	dealloc_command := exec.Command(ssgm_path, "-S", scheduler_address, "-P", scheduler_port, "-dealloc", "-j", rcuda_job_id)
	defer dealloc_command.Run()

	//Read SCAR function input script
	script, err := ioutil.ReadFile(script_path)
	if err != nil {
		panic("Error reading script " + script_path)
	}

	//Create a new file in /tmp/job_id.sh
	filename := "/tmp/" + sqs_job_id + ".sh"
	file, err := os.Create(filename)
	if err != nil {
		panic("Error creating file " + filename)
	}
	defer os.Remove(filename)
	defer file.Close()

	//Copy the script shebang into /tmp/job_id.sh
	_, err = file.Write(script[0:10])
	if err != nil {
		panic("Error copying the detect.sh shebang to /tmp/job_id.sh, " + err.Error())
	}

	//Write the rCUDA data in /tmp/job_id.sh
	for _, split := range rcuda_data_splits {
		_, err := file.WriteString(split + "\n")
		if err != nil {
			panic("Error copying the rCUDA data into /tmp/job_id.sh, " + err.Error())
		}
	}

	//Copy the content of the SCAR function script into /tmp/job_id.sh (except for the shebang)
	_, err = file.Write(script[10:])
	if err != nil {
		panic("Error copying the content of detect.sh into /tmp/job_id.sh, " + err.Error())
	}

	//TODO change scar run for scar invoke? Using curl might be another option
	//Execute the SCAR function using the SCAR CLI
	fmt.Println("Executing scar run...")
	err = exec.Command("scar", "run", "-f", yaml_path, "-s", filename).Run()
	if err != nil {
		panic("Error executing scar run: " + err.Error())
	}
	fmt.Println("scar run successfully executed")

}

