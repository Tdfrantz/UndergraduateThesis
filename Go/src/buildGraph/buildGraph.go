package main

import "golang-set"
import "bufio"
import "os"
import "strings"
import "strconv"
import "fmt"

var graph map[int]mapset.Set
var frequent map[int]bool

func main(){

	args := os.Args[1:]
	graph = make(map[int]mapset.Set)
	frequent = make(map[int]bool)

	if len(args)!=1{
		fmt.Println("Please enter a filename to build the graph from.")
		os.Exit(1)
	}

	if _, err := os.Stat(args[0]); os.IsNotExist(err) {
		fmt.Println("Please enter a valid filename")
		os.Exit(1)
	}

	filename := args[0]

	buildGraph(filename)
	frequentUsers()
	printGraph()
}

func buildGraph(filename string){
file, err := os.Open(filename)

	if err!=nil{
		fmt.Println("Error opening file")
		os.Exit(1)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan(){
		text := strings.TrimSpace(scanner.Text())
		splitText := strings.Split(text, " ")
		for _, user := range splitText{
			userID, _ := strconv.Atoi(user)
			for _, connectUser := range splitText{
				connectID, _ := strconv.Atoi(connectUser)
				if userID!=connectID{
					// If this is the first time we come across this user make a new set in the map
					if graph[userID]==nil{
						graph[userID]=mapset.NewSet()
					}

					graph[userID].Add(connectID)
				}
			}

		}
	}
}

func printGraph(){
	outfile, err := os.Create("graph.txt")
	if err!=nil{
		fmt.Println("There was a problem creating the file")
		os.Exit(1)
	} 

	defer outfile.Close()
	writer := bufio.NewWriter(outfile)

	for user, connectSet := range graph {
		writer.WriteString(strconv.Itoa(user) + ":" + connectSet.String() + "\n")
	}
}

func frequentUsers(){
	frequentFile := "frequentItems_apriori.txt.good"
	file, err := os.Open(frequentFile)

	if err!=nil{
		fmt.Println("Error opening file")
		os.Exit(1)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan(){
		line:=scanner.Text()
		if strings.HasPrefix(line,"Set"){
			users:=between(line)
			usersArr := strings.Split(users,",")
			for _,user := range usersArr{
				userID, _ := strconv.Atoi(user)
				if !frequent[userID]{
					frequent[userID]=true
				}
			}
		}
	}
}

func between(search string) string{
	start := strings.Index(search,"{")
	if start == -1 {
		return ""
	}

	end := strings.Index(search,"}")
	if end == -1{
		return ""
	}

	return search[start+1:end]
}