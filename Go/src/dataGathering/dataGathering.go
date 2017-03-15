package main

import "golang.org/x/oauth2"
import "github.com/google/go-github/github"
import "fmt"
import "os"
import "bufio"
import "strconv"
import "time"

var tokenArray = [...]string  { "db14af379e69078c6521540f992e8dce4aa67832",
								"f354c192e8e869cc4ad324e0002213379dc63e12",
							    "192fcd83c3c9a462bbfc2e42a8782df84e5f20ed",
							    "f48b27c59584100e3b4eaf853c9eae35db74a3e0",
							    "8a1a0b8b6cb518f7b9c88bdaf74fdc7e24a8a4ed",
							    "0db3cdf1d9675f94625a4483a5ce2d45fefcaaa1",
							    "1a47d4525c69d85d8470e55a075c3ffdc46da032",
							    "61e3490551c080845b887c81ccb91161d84608de",
							    "edfff7e8b7e69925e29a35d6a2b7ff445734923a",
							    "5155fb2088bee5562df663171e90ed5bdf50c926",
							    "d1c001653cbb06ad620362c92af2b658422f8603",
							    "a428196b143be45c1a2166208ae46310b33705fd",
							    "deecafc91563f58ede10d612e54a03b51aea1053",
							    "115d2bb5a32f541ab72a18eb6ad5b4cdaaa91b95",
							    "11990be36947c37c341fd48f8fdad4b8994c0b3c",
							    "2f23d9613fdd76b4f4c99dfaa51dec47c570ef76",
							    "2f88041612d354da4611a70fda06147f9b6b20a8",
							    "977a8216e1974b2260271093a703d94cbf535429",
							    "a918340cd9312e860a59af14b031ab06f763e961",
							    "a08da5610b18fe88c8e9922609e6f68b7b717597",
							    "0488a5b81ea780fe4fdcad9404e5d7577501ce16",
							    "3b0cfc1d2e2478c3bfb6fa6e6a7851d4b83e88a7",
							    "8084dea2a3877c41e1b97332e71b3c72ad9081c9",
							    "27c26bda930638184955e1812888093be15c92ef",
							    "7b49955486aa3070079bd3c272fa03973793a6b6",
							    "d7436a484c08ac4294b0d1e649033d7dbdab7afe",
							    "8aea2af0542ff0d04e41850c1f9d18e6a9fc0830",
							    "35b50596a93ceb8f01101631e91a447b2bc17086",
							    "7b16e7a83b1069bfa183466ecd5d05bc66ab0044",
							    "e7cd1a7e348b8a1d0191f69dd06bad8dc1b6fe47",
							    "ca0d535472151c2fc764489a15ba5bc2c2d432a1"}


const sinceFile = "since.txt"

var tokenCounter = 0
var debug = 1
var statusCounter = 10000
const inc = 10000
const maxReposPerFile = 50000
var IDforFileChange = 0

func main(){



	// Create the data file with the current timestamp

	filename := getFileName()
	fmt.Println(filename)
	outfile, err := os.Create(filename)
	if err!=nil {
		fmt.Println("Cannot open output file for writing.")
	}

	writer := bufio.NewWriter(outfile)

	// Generate authentication token and create our client
	client := getNextClient()

	// Set the options for our repository search
	// Will have to save Since as we go 

	opts := &github.RepositoryListAllOptions{}
	opts.PerPage = 100;

	// Check if there is a since.txt file
	// If there is load that and set it as opts.Since

	if _, err := os.Stat(sinceFile); err == nil {
		getSince, errTwo := os.Open(sinceFile)
		if errTwo != nil {
			fmt.Println("Error getting since value.")
			IDforFileChange = maxReposPerFile
		} else {
			reader := bufio.NewReader(getSince)
			lastID,_ := reader.ReadString('\n')
			fmt.Println("Last since ID was " + lastID)
			lastIDInt,_ := strconv.ParseInt(lastID, 10, 32)
			opts.Since = int(lastIDInt)
			IDforFileChange = opts.Since + maxReposPerFile
		}
	} else {
		IDforFileChange = maxReposPerFile
	}

	for {
		repositories, _, _ := client.Repositories.ListAll(opts)
		
		if len(repositories) == 0 {
			fmt.Println("done")
			break
		}

		for _, repository := range repositories{
			id := *repository.ID
			repo, _, err := client.Repositories.GetByID(id)
			if err == nil{
				printString:= repo.String()
				if !needNewFile(id){
					printString = printString + "\n"
				}
				_, errTwo := writer.WriteString(printString)
				if (errTwo!=nil){
					fmt.Println("All the data was not recorded.") 
				}
				writer.Flush()
			}
			
			if checkRate(client)<=0{
				client = getNextClient()
			}

			if needNewFile(id) {
				outfile.Close()
				filename = getFileName()
				fmt.Println(filename)
				outfile, errTwo := os.Create(filename)
				if errTwo!=nil {
					fmt.Println("Cannot open output file for writing.")
				}
				writer = bufio.NewWriter(outfile)
				IDforFileChange += maxReposPerFile
			}
		}
		opts.Since = *repositories[len(repositories)-1].ID
		saveLastID(opts.Since)
	}

	outfile.Close()
}

func checkRate(client *github.Client) int {
	rate,_,rateErr := client.RateLimits()

	if rateErr != nil{
		fmt.Println("Something went wrong checking rate.")
		return -1
	}

	return rate.Core.Remaining
}

func getNextClient() *github.Client {

	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: tokenArray[tokenCounter]},
	)
	tc := oauth2.NewClient(oauth2.NoContext, ts)

	client := github.NewClient(tc)
	currentClient := tokenCounter
	totalTimeWaiting := 0

	for checkRate(client)<=0 {
		fmt.Println("Client " + strconv.Itoa(tokenCounter) + " has no more requests available!")
		tokenCounter++
		if tokenCounter == len(tokenArray){
			tokenCounter=0
		}
		
		ts = oauth2.StaticTokenSource(
			&oauth2.Token{AccessToken: tokenArray[tokenCounter]},
		)
		tc = oauth2.NewClient(oauth2.NoContext, ts)

		client = github.NewClient(tc)

		// if we go all the way around, wait a minute before checking them all again
		if tokenCounter==currentClient {
			fmt.Println("No available clients, waiting 1 minute. Previous time waiting was " + strconv.Itoa(totalTimeWaiting) + " minutes.")
			totalTimeWaiting++
			time.Sleep(time.Minute)
		}
	}

	if debug == 1 {
		fmt.Println("Now using token number: " + strconv.Itoa(tokenCounter))
	}

	return client
}

func saveLastID(id int) {
	writeSince, err := os.Create(sinceFile)
	if err!=nil{
		fmt.Println("Could not open since file for writing")
	}
	defer writeSince.Close()
	sinceWriter := bufio.NewWriter(writeSince)
	sinceWriter.WriteString(strconv.Itoa(id))
	sinceWriter.Flush()
	if id > statusCounter {
		fmt.Println("Currently at ID " + strconv.Itoa(id))
		statusCounter += inc
	}
}

func getFileName() string{
	return "data/repoData_" + time.Now().Format("20060102150405") + ".dat"
}

func needNewFile(id int) bool{
	return id >= IDforFileChange
}

