path="C:\\Users\\David\\Desktop\\DevOps2019\\datascience\\datasets\\SparkScala"
bookPath=path+"\\book.txt"

print("Hello World")
print(bookPath)

# Open the file in read mode
text = open(bookPath, "r", encoding="utf8")
  
# Create an empty dictionary
d = dict()
  
# Loop through each line of the file
for line in text:
    # Remove the leading spaces and newline character
    line = line.strip()
  
    # Convert the characters in line to
    # lowercase to avoid case mismatch
    line = line.lower()
  
    # Split the line into words
    words = line.split(" ")
                         
  
    # Iterate over each word in line
    for word in words:
        # Check if the word is already in dictionary
        if word in d:
            # Increment count of word by 1
            d[word] = d[word] + 1
        else:
            # Add the word to dictionary with count 1
            d[word] = 1
  
# Print the contents of dictionary
for key in list(d.keys()):
    print(key, ":", d[key])

    
# #import dependencies
# from pyspark import SparkConf,SparkContext
# conf=SparkConf().setMaster("local").setAppName("WordCount")
# sc=SparkContext(conf=conf)
# input=sc.textFile(book)
# words=input.flatMap(normalizeWords)
# #count the words
# wordCounts=words.countByValue()

# #sort the words
# sortedResults=sorted(wordCounts.items(),key=lambda x:x[1],reverse=True)

# #display the results
# for word,count in sortedResults:
#     cleanWord=word.encode('ascii','ignore')
#     if(cleanWord):
#         print(cleanWord.decode()+"\t"+str(count))



# #return a list of words
# def normalizeWords(text):
#     return re.compile(r'\W+',re.UNICODE).split(text.lower())

# #load the book
# input = sc.textFile(book)

# #count the words
# wordCounts = input.flatMap(normalizeWords) \
#     .map(lambda x: (x, 1)) \
#     .reduceByKey(lambda x, y: x + y) \
#     .map(lambda x: (x[1], x[0])) \
#     .sortByKey()

# #print the results
# results = wordCounts.collect()

# for result in results:
#     count = str(result[0])
#     word = result[1].encode('ascii', 'ignore')
#     if (word):
#         print(word.decode() + ":\t\t" + count)


#save the results
#wordCounts.saveAsTextFile("C:\Users\David\Desktop\DevOps2019\datascience\datasets\SparkScala\wordCounts")