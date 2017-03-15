package main

import "golang.org/x/oauth2"
import "github.com/google/go-github/github"
import "fmt"
import "os"
import "bufio"
import "time"
import "strconv"
import "log"

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

var tokenCounter = 0
var debug = 1
var organization = "twitter"

//userIDMap is ID -> bool
// usernameMap is Name -> ID
// usernameEmail is ID -> email
var userIDMap map[int]bool
var usernameMap map[string]int
var usernameEmail map[int]string
var userCount = 0
var logger *log.Logger

// savedRepos keeps track of which repos have been printed as a basket
// When the repo gets printed the ID is saved in this map
var savedRepos map[int]bool
var repoCount = 0
const maxRepos = 100000

var since = time.Date(2016,time.January,1, 0, 0, 0, 0, time.UTC)
var until = time.Date(2017,time.January,1, 0, 0, 0, 0, time.UTC)

func main(){
	createLogger()

	//initialize the maps
	userIDMap = make(map[int]bool)
	usernameMap = make(map[string]int)
	usernameEmail = make(map[int]string)
	savedRepos = make(map[int]bool)

	//initilize the writer object
	filename := getOrganizationFileName(organization)
	outfile, err := os.Create(filename)
	if err!=nil {
		logger.Println("Cannot open output file for writing:" + err.Error())
	} else {
		logger.Println("Collection data for organization " + organization)
		logger.Println("Printing to file " + filename)
	}
	defer outfile.Close()
	writer := bufio.NewWriter(outfile)

	// Get all the repos for the chosen organization
	// This is what we will seed the bulk of the data generation with
	client := getNextClient()
	org,_,_:= client.Organizations.Get(organization)
	repositories := getNewReposForOrganization(*org.Login)
	newUsers := processReposGetUsers(repositories, writer)

	// The main loop to expand the graph with
	for repoCount < maxRepos{
		newNewUsers := make(map[int]bool)
		for user, _ := range newUsers{
			repositories = getNewReposForUser(user)
			tempNewUsers := processReposGetUsers(repositories,writer)
			for tempUser, _ := range(tempNewUsers){
				if newNewUsers[tempUser]==false{
					newNewUsers[tempUser]=true
				}
			}
		}
		newUsers = make(map[int]bool)
		for user, _ := range newNewUsers{
			newUsers[user]=true
		}
	}


	//printReposForOrganization(*org.Login)
	
	// Once we have an expanded sweep of users print them all to file and see what we get
	// printUsernames(*org.Login)
}

func checkRate(client *github.Client) int {
	rate,_,rateErr := client.RateLimits()

	if rateErr != nil{
		logger.Println("Something went wrong checking rate:" + rateErr.Error())
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
		logger.Println("Client " + strconv.Itoa(tokenCounter) + " has no more requests available!")
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
			logger.Println("No available clients, waiting 1 minute. Previous time waiting was " + strconv.Itoa(totalTimeWaiting) + " minutes.")
			fmt.Println("Waiting for available client for " + strconv.Itoa(totalTimeWaiting) + " minutes.")
			totalTimeWaiting++
			time.Sleep(time.Minute)
		}
	}

	logger.Println("Now using token number: " + strconv.Itoa(tokenCounter))
	return client
}

func getNewReposForOrganization(org string) []*github.Repository{
	client := getNextClient()
	var repositories []*github.Repository
	opts := &github.RepositoryListByOrgOptions{}
	opts.Page = 1
	for{
		if checkRate(client)<=0{
			client = getNextClient()
		}
		repos, response, err := client.Repositories.ListByOrg(org, opts)
		if err==nil{
			if debug==1{
				fmt.Printf("currentPage: %v\tlastPage: %v\n",opts.Page,response.LastPage)
			}

			for _,repository := range repos{
				repoID := *repository.ID
				if !savedRepos[repoID]{
					repositories = append(repositories,repository)
				}
			}

			opts.Page++
		}

		if response.LastPage==0{
			break
		}
	}

	return repositories
}

func getNewReposForUser(userID int) []*github.Repository{
	client := getNextClient()

	// First get the user's login to use in the API call
	user, _, _ := client.Users.GetByID(userID)
	var repositories []*github.Repository
	opts := &github.RepositoryListOptions{}
	opts.Page = 1
	if user.Login!=nil{
		login := *user.Login

		for {
			if checkRate(client)<=0{
				client = getNextClient()
			}
			repos, response, err := client.Repositories.List(login,opts)
			if err==nil{
				if debug==1{
					fmt.Printf("currentPage: %v\tlastPage: %v\n", opts.Page, response.LastPage)
				}
				for _,repository := range repos{
					repoID := *repository.ID
					if !savedRepos[repoID]{
						repositories = append(repositories,repository)
					}
				}

				opts.Page++
			}

			if response.LastPage==0{
				break
			}
		}
	}

	return repositories
}

func processReposGetUsers(repositories []*github.Repository, writer *bufio.Writer) map[int]bool{
	client := getNextClient()
	newUsers := make(map[int]bool)
	for _,repository := range repositories{
		
		// save the unique committers in this map
		committers := make(map[int]bool)
		
		// set initial commit search options
		commitOpts := &github.CommitsListOptions{}
		commitOpts.Page = 1
		commitOpts.Since = since
		commitOpts.Until = until

		// need the owner and repo name for the search API
		owner := *repository.Owner.Login
		name := *repository.Name

		// check all the commits for the repo
		for{
			if checkRate(client)<=0{
				client = getNextClient()
			}
			commits, commitResponse, _ := client.Repositories.ListCommits(owner,name,commitOpts)
			
			if debug==1{
				fmt.Printf("Current commit lastPage: %v, current commit page: %v. \n", commitResponse.LastPage, commitOpts.Page)
			}

			commitOpts.Page++

			for _, commit := range commits{
				committerID := 0
				if commit.Committer!=nil{
					committer := *commit.Committer
					if committer.ID!=nil{
						committerID = *committer.ID
					}
					
					// If we have a new committer add them to the committers array
					if !committers[committerID]{
						committers[committerID]=true
					}

				}else if commit.Author!=nil{
					committer := *commit.Author
					if committer.ID!=nil{
						committerID = *committer.ID
					} 

					// If we have a new committer add them to the committers array
					if !committers[committerID]{
						committers[committerID]=true
					}
				}
			}
			
			if commitResponse.LastPage==0{
				break
			}
		}

		if debug==1{
			fmt.Printf(name + " has %v unique committers.\n", len(committers))
		}

		// If there are at least 2 committers then we will print the repo to file as a basket
		if len(committers)>=2{
			for committer,_ := range committers{
				if !userIDMap[committer]{
					newUsers[committer]=true
				}
			}
			printBasket(writer, committers)

		}
	}

	return newUsers
}

func printBasket(writer *bufio.Writer, committers map[int]bool){
	for committer, _ := range committers{
		addUserID(committer)
		if debug==1{
			fmt.Print(committer)
			fmt.Print(" ")
		}

		writer.WriteString(strconv.Itoa(committer))
		writer.WriteString(" ")
	}

	if debug==1{
		fmt.Println()
	}

	writer.WriteString("\n")
	writer.Flush()
	repoCount++
}

func printReposForOrganization(org string){
	filename := getOrganizationFileName(organization)
	outfile, err := os.Create(filename)
	if err!=nil {
		logger.Println("Cannot open output file for writing:" + err.Error())
	} else {
		logger.Println("Collection data for organization " + organization)
		logger.Println("Printing to file " + filename)
	}
	writer := bufio.NewWriter(outfile)
	opts := &github.RepositoryListByOrgOptions{}
	opts.Page = 1
	client := getNextClient()
	for {

		repos, response, err := client.Repositories.ListByOrg(org, opts)
		lastPage := 0

		if err==nil{
			
			lastPage = response.LastPage
			if debug==1{
				fmt.Printf("Current repository lastPage: %v, current repository page: %v. \n", lastPage, opts.Page)
			}

			for _, repository := range repos{
				//need the user for the repo
				owner := *repository.Owner.Login
				name := *repository.Name
				var committers map[string]bool
				var committersEmail map[string]string
				committers = make(map[string]bool)
				committersEmail = make(map[string]string)
				//use RepoCommit structure to get the commits 

				if checkRate(client)<=0{
					client = getNextClient()
				}
				// Set since to a year
				commitOpts := &github.CommitsListOptions{}
				commitOpts.Page = 1
				commitOpts.Since = time.Date(2016,time.January,1, 0, 0, 0, 0, time.UTC)
				commitOpts.Until = time.Date(2017,time.January,1, 0, 0, 0, 0, time.UTC)

				// Get the commits for that repo
				
				for{
					if checkRate(client)<=0{
						client = getNextClient()
					}
					commits, commitResponse, _ := client.Repositories.ListCommits(owner,name,commitOpts)
					
					if debug==1{
						fmt.Printf("Current commit lastPage: %v, current commit page: %v. \n", commitResponse.LastPage, commitOpts.Page)
					}

					commitOpts.Page+=1

					for _, commit := range commits{
						commitWrap := *commit.Commit
						committer := *commitWrap.Committer
						// If we have a new committer add them to the committers array
						if !committers[*committer.Name]{
							committers[*committer.Name]=true
							committersEmail[*committer.Name]=*committer.Email
						}
					}
					
					if commitResponse.LastPage==0{
						break
					}
				}

				// Print the committers array if at least 2 committers
			
				if debug==1{
					fmt.Printf(name + " has %v unique committers.\n", len(committers))
				} 	

				if len(committers)>=2{
					for committer, _ := range committers{

						userid := checkUsername(committer, committersEmail[committer])

						if debug==1{
							fmt.Print(userid)
							fmt.Print(" ")
						}

						writer.WriteString(strconv.Itoa(userid))
						writer.WriteString(" ")
						
					}

					if debug==1{
						fmt.Println()
					}
					
					writer.WriteString("\n")
					writer.Flush()
				}
			}
		}

		opts.Page+=1
		if opts.Page>lastPage{
			break
		}
	}

	// With all the users from the organization we will do a second sweep for repositories
	
	// create a copy of the usernames we have already, since we want to add to usernameMap	

	users := copyUsernameMapToArray()
	
	for _,user := range(users){
		if checkRate(client)<=0{
			client = getNextClient()
		} 
		// Look up the repositories each user belongs to using search api 
		searchOpts := &github.SearchOptions{}
		searchOpts.Page=1
		searchQuery := "user:"+user+"+pushed:>2015-12-31"
		result, response, err := client.Search.Repositories(searchQuery, searchOpts)
		userRepos := result.Repositories
		if err==nil{
			for response.LastPage!=0{
				for _, repository := range(userRepos){
					repoOrg := *repository.Organization
					// If the repo is in the same organization we initally seed with then we will skip it this pass
					if *repoOrg.Login!=org{
						owner := *repository.Owner.Login
						name := *repository.Name
						var committers map[string]bool
						var committersEmail map[string]string
						committers = make(map[string]bool)
						committersEmail = make(map[string]string)
						//use RepoCommit structure to get the commits 

						if checkRate(client)<=0{
							client = getNextClient()
						}
						// Set since to a year
						commitOpts := &github.CommitsListOptions{}
						commitOpts.Page = 1
						commitOpts.Since = time.Date(2016,time.January,1, 0, 0, 0, 0, time.UTC)
						commitOpts.Until = time.Date(2017,time.January,1, 0, 0, 0, 0, time.UTC)

						// Get the commits for that repo
						if checkRate(client)<=0{
							client = getNextClient()
						}
						commits, commitResponse, _ := client.Repositories.ListCommits(owner,name,commitOpts)
								
						for commitResponse.LastPage!=0{

							if debug==1{
								fmt.Printf("Current commit lastPage: %v, current commit page: %v. \n", commitResponse.LastPage, commitOpts.Page)
							}

							commitOpts.Page+=1

							for _, commit := range commits{
								commitWrap := *commit.Commit
								committer := *commitWrap.Committer
								// If we have a new committer add them to the committers array
								if !committers[*committer.Name]{
									committers[*committer.Name]=true
									committersEmail[*committer.Name]=*committer.Email
								}
							}
						}
						if debug==1{
							fmt.Printf(name + " has %v unique committers.\n", len(committers))
						} 	

						if len(committers)>=2{
							for committer, _:= range committers{

								userid := checkUsername(committer,committersEmail[committer])

								if debug==1{
									fmt.Print(userid)
									fmt.Print(" ")
								}

								writer.WriteString(strconv.Itoa(userid))
								writer.WriteString(" ")
								
							}

							if debug==1{
								fmt.Println()
							}
							
							writer.WriteString("\n")
							writer.Flush()
						}
					}
				}
			}
		}


	}

}

func getOrganizationFileName(org string) string{
	return "data/" + org + "_data.dat"
}

func contains(data []string, toFind string) bool{

	for _, i := range data{
		if i==toFind{
			return true
		}
	}
	return false
}

func addUserID (id int) bool{
	if userIDMap[id]==false{
		userIDMap[id]=true
	}

	return userIDMap[id]
}

func checkUsername(username string, email string) int{
	if usernameMap[username]==0{
		userCount++
		// usernameMap is Name -> ID
		// usernameEmail is ID -> email
		usernameMap[username] = userCount
		usernameEmail[userCount] = email

	}
	return usernameMap[username]
}

func printUsernames(org string){
	filename := "data/" + org + "_usernames.dat"
	outfile, err := os.Create(filename)
	if err!=nil {
		logger.Println("Cannot open output file for writing:" + err.Error())
	}
	writer := bufio.NewWriter(outfile)
	for key, value := range(usernameEmail) {
		writer.WriteString(value + " " + strconv.Itoa(key) + "\n")
	}
}

func createLogger(){
	logloc := "logs/" + organization + "_" + time.Now().Format("20060102150405") + ".txt"
	logFile, err := os.Create(logloc)
	if err != nil {
		panic(err)
	}
	logger = log.New(logFile,"", log.Ldate | log.Ltime)
}
	
func copyUsernameMapToArray() []string{
	users := make([]string,0)
	for user, _ := range usernameMap{
		users = append(users,user)
	}
	return users
}