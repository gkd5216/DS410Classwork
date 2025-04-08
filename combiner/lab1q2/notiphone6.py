from mrjob.job import MRJob 

class WordCount(MRJob):
    """ The goal of this mapreduce job is to count words with at least 5 characters
    that appear more times than they have characters """
    def mapper(self, key, line):
        """ This mapper expects the following information as input:
        key: This mapper emits the word with at least 5 characters as the key
        value: This mapper emits an integer as the value
        and its goal is to yield the word with at least 5 characters that appear more
        times than they have characters """
        words = line.split()
        for w in words:
            if len(w) >= 5: #Words with at least 5 characters
                yield (w, 1)

    def combiner(self, key, value_list):
        """ This combiner expects the following information as input:
        key: This combiner emits the word from the mapper as the key
        value: This combiner emits the sum of words from output of mapper
        and its goal is to yield words with at least 5 characters appearing
        more times than characters """
        yield key, sum(value_list)

    def reducer(self, key, value_list):
        """ This reducer expects the following information as input:
        key: This reducer emits the word as the key
        value: This reducer emits the count of words that appear more times
        than characters
        and its goal is to yield the word with at least 5 characters that appear
        more times than characters """
        count = sum(value_list)
        if count > len(key):
            yield (key, count)

if __name__ == '__main__':
    WordCount.run()

### Bugs detected and fixed ###
# 1. Combiner changing output; added sum to value_list in combiner
