package main

import "fmt"
import "bufio"
import "os"
import "strings"
import "strconv"
import "golang-set"
import "time"
import "hash/fnv"

const MEMORY = ((1*1024*1024)/4) //1 MB
//const MEMORY = 32;

func main(){

	start := time.Now()
	debug:=0
	//minSupport := 2
	minSupportMult:=0.015
	maxTupleSize:=4
	filename:="data/google_data.dat"
	var PCYTable [MEMORY]int32
	var PCYBmap [MEMORY/32]int32

	outfile, err := os.Create("frequentItems_PCY.txt")
	if err!=nil {
		fmt.Println("Cannot open output file for writing.")
	}

	defer outfile.Close()
	writer := bufio.NewWriter(outfile)

	//first pass - store the item counts, and hash the pairs to buckets

	//MAP FOR ITEM COUNT
	var firstPass map[string]int
	firstPass = make(map[string]int)

	f, err := os.Open(filename)
	if err!=nil{
		fmt.Println("Cannot open data file.")
	} 
	defer f.Close()

	// READ EVERY LINE
	scanner := bufio.NewScanner(f)
	baskets := make([]string,0);

	for scanner.Scan() {
		baskets = append(baskets, strings.TrimSpace(scanner.Text()))
	}

	//SET THE SUPPORT THRESHOLD RELATIVE TO THE NUMBER OF BASKETS
	minSupport := int(minSupportMult*float64(len(baskets)))
	fmt.Println(minSupport)

	for basketIndex := range baskets{
		line :=  baskets[basketIndex]
		splitLine := strings.Split(line," ")

		// ADD EACH COUNT TO THE MAP
		for split := range splitLine{
			firstPass[splitLine[split]]+=1
			
			//HASH EACH PAIR AND INCREMENT THE HASHMAP
			for makePairs:=split+1;makePairs<len(splitLine);makePairs++{
				hashValue := (hash(splitLine[split])+hash(splitLine[makePairs]))%MEMORY
				if debug==1{
					fmt.Printf("Making pairs %v,%v. The hash value is %v. \n", splitLine[split],splitLine[makePairs], hashValue)
				}
				PCYTable[hashValue]+=1
			}
		}
	}

	// Remove all entries from firstPass that are below minSupport
	for key, value := range firstPass {
    	if value<minSupport {
    		delete(firstPass,key)
    	}
    }

	if debug==1{
		//PRINT THE REMAINING ENTRIES
		fmt.Println("\n--------FREQUENT ITEM COUNT--------")
		for key, value := range firstPass {
    		fmt.Println("Key:", key, "Value:", value)
    	}
	}

	// MAKE AN ARRAY OF ALL FREQUENT ITEMS
	frequentItems := make([]string,0)
	for key,_ := range firstPass {
		frequentItems = append(frequentItems,key)
	}

	firstPass = nil //clear the firstPass map to free memory

	// Make the bitmap
	for pos := range PCYTable{
		if PCYTable[pos] >= int32(minSupport){
			PCYBmap[pos/32] = PCYBmap[pos/32] | (1 << uint(pos%32))
		}
	}

	frequentSet := make(map[mapset.Set]int)

	for tupleSize:=2;tupleSize<=maxTupleSize;tupleSize++{

		candidateSet:=make([]mapset.Set,0);

		if tupleSize==2{
			var alreadyChosen map[string]bool
			alreadyChosen = make(map[string]bool)
			for key := range frequentItems{	
				alreadyChosen[frequentItems[key]]=true;
				for key2 := range frequentItems{
					_, check := alreadyChosen[frequentItems[key2]]
					if check == false{
						//NEW IN PCY - Check if the pair hashes to a frequent bucket
						hashValue := (hash(frequentItems[key])+hash(frequentItems[key2]))%MEMORY
						if (PCYBmap[hashValue/32] & (1 << uint(hashValue%32)) == (1 << uint(hashValue%32))){
							tempSet:=mapset.NewSet()
							tempSet.Add(frequentItems[key])
							tempSet.Add(frequentItems[key2])
							candidateSet = append(candidateSet, tempSet)
						}
					}
				}
			}

			//clear the alreadyChosen map
			alreadyChosen = nil
		} else {
			for key, _ := range frequentSet{
				for index := range frequentItems{
					if key.Contains(frequentItems[index])==false{
						convertSet := mapset.NewSet()
						convertSet.Add(frequentItems[index])
						tempSet := convertSet.Union(key)
						add := true
						for index2 := range candidateSet{
							if candidateSet[index2].Equal(tempSet){
								add=false
								break
							}
						}
						if add{
							candidateSet = append(candidateSet, tempSet)
						}
					}
				}
			}		

		//CLEAR THE FREQUENT SETS VARIABLE
			frequentSet = make(map[mapset.Set]int)

		}

		if debug == 1{
			fmt.Println("\n----------PRINTING CANDIDATE PAIRS----------")
				for pair :=range candidateSet{
				fmt.Println(candidateSet[pair])
			}
		}

		for basketIndex := range baskets{
			line := baskets[basketIndex]

			// SPLIT EACH LINE AND MAKE IT INTO A SET
			splitLine := strings.Split(line," ")	//retail.dat is split on space
			lineSet := mapset.NewSet()
			for split := range splitLine{
				lineSet.Add(splitLine[split])
			}

			for index := range candidateSet{
				
				// IF THE INTERSECTION OF A CANDIDATE SET AND THE LINE EQUALS THE CANDIDATE SET
				// THEN WE ADD IT AS A FREQUENT PAIR

				if debug==1{
					fmt.Print("Comparing the intersection of: ")
					fmt.Print(candidateSet[index])
					fmt.Print(" and ")
					fmt.Println(lineSet)
				}

				checkSet := lineSet.Intersect(candidateSet[index])

				if checkSet.Equal(candidateSet[index]){
					frequentSet[candidateSet[index]]+=1
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
	   		if value<minSupport{
	   			delete(frequentSet,key)
	   		}
	   	}

	   	//Clear the old bitmap
	   	for i := range PCYBmap{
	   		PCYBmap[i]=0;
	   	}

	   	//Make the new bitmap


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
		writer.Flush()
		end := time.Now()
		fmt.Printf("Done printing tuples of size %v in %v seconds.\n", tupleSize, end.Sub(start))
	}


}

func hash(s string) uint32{
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32();
}