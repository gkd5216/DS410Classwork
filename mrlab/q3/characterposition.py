from mrjob.job import MRJob 

class WordCount(MRJob):  
    def mapper(self, key, line):
        words = line.split()
        for w in words:
            for i in range(len(w)):
                character = w[i] 
                yield ((character, i), 1)

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    WordCount.run()
