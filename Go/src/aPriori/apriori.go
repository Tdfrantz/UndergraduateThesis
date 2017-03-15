package main

import "fmt"
import "bufio"
import "os"
import "strings"
import "strconv"
import "golang-set"
import "time"
import "log"

//inputs: file, delimeter, minSupport, maxTupleSize

func main() {

	args := os.Args[1:]
	if len(args)!=2{
		fmt.Println("To use: aPriori.exe <filename> <minSupport>")
		os.Exit(1)
	}

	if _, err := os.Stat(args[0]); os.IsNotExist(err) {
		fmt.Println("Please enter a valid filename")
		os.Exit(1)
	}

	filename:=args[0]

	minSupportMult,err := strconv.ParseFloat(args[1],64)
	if err!=nil{
		fmt.Println("Please enter a valid support threshold (a float between 0 and 1)")
		os.Exit(1)
	} else if minSupportMult <=0 || minSupportMult >= 1 {
		fmt.Println("Please enter a valid support threshold (a float between 0 and 1)")
		os.Exit(1)
	}

	logloc := "logs/aPriori_" + time.Now().Format("20060102150405") + ".txt"
	logFile, err := os.Create(logloc)
	if err != nil {
		panic(err)
	}
	logger := log.New(logFile,"", log.Ldate | log.Ltime)
	logger.Println("Starting aPriori run for " + filename)
	start := time.Now()
	logger.Println("Run starting at " + start.String())
	debug := 0
	//minSupport:=2
	//minSupportMult:=0.015
	maxTupleSize:=6
	//filename:="data/google_data.dat"

	outfile, err := os.Create("frequentItems_apriori.txt")
	if err!=nil {
		fmt.Println("Cannot open output file for writing.")
	}
	defer outfile.Close()
	writer := bufio.NewWriter(outfile)

	
	/*******************FIRST PASS*********************/

	//the map that we will store the count of each word in
	var firstPass map[int]int
	firstPass = make(map[int]int)

	// OPEN THE FILE
	f, err := os.Open(filename)	//retail.dat for now
	if err!=nil {
		fmt.Println("Cannot open file.")
	}
	defer f.Close()

	// READ EVERY LINE
	scanner := bufio.NewScanner(f)
	baskets := make([]string,0)

	for scanner.Scan(){
		baskets = append(baskets, strings.TrimSpace(scanner.Text()))	//TrimSpace trims whitespace off the ends
	}

	//SET THE SUPPORT THRESHOLD RELATIVE TO THE NUMBER OF BASKETS
	minSupport := int(minSupportMult*float64(len(baskets)))
	logger.Printf("%v is the minSupportMult. There are %v baskets so the minimum threshold is %v\n", minSupportMult, len(baskets), minSupport)
	logger.Println("Now starting first pass.")
	for _,basket := range baskets{ 
		// SPLIT EACH LINE
		splitLine := strings.Split(basket," ")	//retail.dat is split on space
		
		// ADD EACH COUNT TO THE MAP
		for _,split := range splitLine{
			splitInt,_ := strconv.Atoi(split)
			firstPass[splitInt]+=1
		}
	}
	
	if debug==1{
		//PRINT ALL ENTRIES
		fmt.Println("--------PRINTING ALL ENTRIES--------")
		for key, value := range firstPass {
    		fmt.Println("Key:", key, "Value:", value)
    	}
	}

	// REMOVE ALL ENTRIES THAT ARE BELOW minSupport
	logger.Println("Removing entries below minSupport")
	for key, value := range firstPass {
    	if value<minSupport {
    		delete(firstPass,key)
    	}
    }

	if debug==1{
		//PRINT THE REMAINING ENTRIES
		fmt.Println("\n--------PRINTING REMAINING ENTRIES--------")
		for key, value := range firstPass {
    		fmt.Println("Key:", key, "Value:", value)
    	}
	}

	// MAKE AN ARRAY OF ALL FREQUENT ITEMS
	frequentItems := make([]int,0)

	for key,_ := range firstPass {
		frequentItems = append(frequentItems,key)
	}

	firstPass = make(map[int]int) //clear the firstPass map to free memory
	
	//DECLARE THE FREQUENT SET OUT HERE, SO IT'S NOT OVERWRITTEN EVERY TIME
	//Maybe can optimize here, a count is not needed for aPriori, so a map may not be needed. Still a nice data structure though

	frequentSet := make(map[mapset.Set]int)

	for tupleSize:=2;tupleSize<=maxTupleSize;tupleSize++{

		// CREATE A LIST OF ALL CANDIDATE TUPLES
		candidateSet := make([]mapset.Set,0)
		logger.Printf("Now building candidate set for tuples of size %v\n",tupleSize)
		//TO MAKE THE INITIAL PAIRS COMBINE ALL ELEMENTS IN THE FREQUENT ITEMS
		if tupleSize==2{		
			var alreadyChosen map[int]bool
			alreadyChosen = make(map[int]bool)
			for _,item1 := range frequentItems{
				alreadyChosen[item1]=true;
				for _,item2 := range frequentItems{
					_, check := alreadyChosen[item2]
					if check == false{
						tempSet:=mapset.NewSet()
						tempSet.Add(item1)
						tempSet.Add(item2)
						candidateSet = append(candidateSet, tempSet)
					}
				}
			}

			//clear the alreadyChosen map
			alreadyChosen = nil;
		} else	{

		//TO MAKE SUBSEQUENT SETS COMBINE THE FREQUENT SETS WITH EVERY FREQUENT ITEM

			logger.Println("Now combining " + strconv.Itoa(len(frequentSet)) + " frequent sets with " + strconv.Itoa(len(frequentItems)) + " frequent items")

			for key, _ := range frequentSet{
				logger.Println("Combining set " + key.String())
				for _, item := range frequentItems{
					if key.Contains(item)==false{
						convertSet := mapset.NewSet()
						convertSet.Add(item)
						tempSet := convertSet.Union(key)
						add := true
						for _, set := range candidateSet{
							if set.Equal(tempSet){
								add=false
								break
							}

							// FURTHER PRUNE THE CANDIDATE SETS TO IMPROVE TIME
							// NEED TO CHECK THAT ALL POSSIBLE COMBINATIONS OF SETS ARE FREQUENT
							// CAN CHECK ALL SUBSETS BY REMOVING ONE FROM THE SET AT A TIME

							checkSet := tempSet.Clone()
							checkIter := tempSet.ToSlice()
							for _, check := range(checkIter){
								checkSet.Remove(check)
	
								// need to check every frequent set manually I think
								// map lookup isn't working if I create a new set and look it up
								if !checkSet.Equal(key){
									for freq, value := range(frequentSet){
										if checkSet.Equal(freq){
											
											if debug == 1 {
												fmt.Println("The value of " + checkSet.String() + " is " + strconv.Itoa(value))
											}

											if value<minSupport{
												add = false
												break
											}
										}
									}
								}
								checkSet.Add(check)
							}
						}
						if add{
							candidateSet = append(candidateSet, tempSet)
						}
					}
				}
			}

		//CLEAR THE FREQUENT SETS VARIABLE
			logger.Println("Clearing the previous frequent sets")
			frequentSet = make(map[mapset.Set]int)
		}

		if debug == 1{
			fmt.Println("\n--------PRINTING CANDIDATE PAIRS----------")
				for _,set :=range candidateSet{
				fmt.Println(set)
			}
		}

		/***************************SECOND PASS***************************/

		logger.Printf("Now starting second pass for tuples of size %v\n",tupleSize)
		printLog := int(len(baskets)/50)
		if printLog == 0{
			printLog =1
		}

		for index, basket := range baskets{
			line := basket

			// Print to logger every 2% of the way
			if index%printLog==0{
				logger.Printf("Checking basket number %v\n", index)
			}

			// SPLIT EACH LINE AND MAKE IT INTO A SET
			splitLine := strings.Split(line," ")	//retail.dat is split on space
			lineSet := mapset.NewSet()
			for _,split := range splitLine{
				splitInt, _ := strconv.Atoi(split)
				lineSet.Add(splitInt)
			}

			// COMPARE THE INTERSECTION OF EACH CANDIDATE SET WITH THE LINE'S SET
			for _,set := range candidateSet{
				
				// IF THE CANDIDATE SET IS A SUBSET OF THE LINE SET THEN WE'RE GOOD

				if debug==1{
					fmt.Print("Comparing the intersection of: ")
					fmt.Print(set)
					fmt.Print(" and ")
					fmt.Print(lineSet)
				}

				if set.IsSubset(lineSet){
					if debug==1{
						fmt.Println(" YES")
					}
					frequentSet[set]+=1
				}else{
					if debug==1{
						fmt.Println(" NO")
					}
				}
			}
		}

		if debug==1{
			//PRINT ALL ENTRIES
			fmt.Println("--------PRINTING ALL ENTRIES--------")
			for key, value := range frequentSet {
	   			fmt.Println("Key:", key, "Value:", value)
	   		}
		}
		
		// REMOVE ALL ENTRIES THAT ARE BELOW minSupport
		for key, value := range frequentSet {
	   		if value<minSupport {
	   			delete(frequentSet,key)
	   		}
	   	}

	   	// WRITE THE FREQUENT SET ITEMS TO FILE
		outString := "Printing tuples of size " + strconv.Itoa(tupleSize) + ":\n"
		_, err = writer.WriteString(outString)

		for key, value := range frequentSet{
			outString = key.String() + " : " + strconv.Itoa(value) + "\n"
			_, err = writer.WriteString(outString)
		} 

		_,err = writer.WriteString("\n")

	   	if debug==1{
			//PRINT THE REMAINING ENTRIES
			fmt.Println("\n--------PRINTING REMAINING ENTRIES--------")
			for key, value := range frequentSet {
	   			fmt.Println("Key:", key, "Value:", value)
	   		}
		}

		// REBUILD THE FREQUENT ITEMS LIST WITH ONLY ITEMS THAT APPEAR IN A FREQUENT SET


		logger.Println("Trimming down the frequent items list.")
		logger.Println("Old frequent items list contains " + strconv.Itoa(len(frequentItems)) + " items.")

		frequentItems = make([]int,0)

		for key,_ := range frequentSet{
			valsToAdd := key.ToSlice()
			for _, val := range valsToAdd{
				add:=true
				for _, i:= range frequentItems{
					if val==i{
						add=false
						break
					}
				}
				if add{
					if check, ok := val.(int); ok{
						frequentItems = append(frequentItems,check)
					}
				}
			}
		}

		logger.Println("New frequent items list contains " + strconv.Itoa(len(frequentItems)) + " items.")

		if debug==1{
			fmt.Println("\n--------PRINTING REFINED FREQUENT ITEMS LIST------------")
			for _, i := range frequentItems{
				fmt.Println(i)
			}
		}

		writer.Flush()
		end := time.Now()
		fmt.Printf("Done printing tuples of size %v in %v seconds.\n", tupleSize, end.Sub(start))
	}

}
