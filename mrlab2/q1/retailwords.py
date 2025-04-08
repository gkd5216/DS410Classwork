from mrjob.job import MRJob

class WordCount(MRJob):
    def mapper(self, key, line):
        words = line.split("\t") #Split by tab
        if words[0] == 'InvoiceNo': #Skip header
            pass
        else:
            description = words[2].strip() #Gets third indices of file
            description_split = description.split() #Splits description into separate words
            for w in description_split: #Iterate through the words
                clean_word = w.strip() #No whitespace
                yield(clean_word, 1)
    
    def reducer(self, key, values):
        yield(key, sum(values)) #Return counts for each word

if __name__ == '__main__':
    WordCount.run() #MRJob version

