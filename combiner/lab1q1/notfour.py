from mrjob.job import MRJob 

class WordCount(MRJob):
    """ The goal of this mapreduce job is to use the hdfs dataset /datasets/wap to compute 
    the count for words that have 5 characters or more """
    def mapper(self, key, line):
        """ This mapper expects the following information as input:
        key: This mapper emits the word that is being counted as the key
        value: This mapper emits the number of words with 5 characters or more as the value
        and its goal is to yield the count for words that have 5 characters or more """
        words = line.split()
        for w in words:
            if len(w) >= 5:
                yield (w, 1)

    def combiner(self, key, value_list):
        """ This combiner expects the following information as input:
        key: This combiner emits the word being counted as the key
        value: This combiner emits the count of words with 5 characters or more as the value
        and its goal is to yield the count for words with 5 characters or more"""
        yield key, sum(value_list)

    def reducer(self, key, value_list):
        """ This reducer expects the following information as input:
        key: This reducer emits the word being counted as the key
        value: This reducer emits the count of words with 5 characters or more as the value
        and its goal is to yield the count for words with 5 characters or more"""
        yield key, sum(value_list)

if __name__ == '__main__':
    WordCount.run()

### Bugs detected and fixed ###
# 1. Reducer not adding up the total count for words; Added sum to value_list variable when yielding
