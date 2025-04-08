from mrjob.job import MRJob   

class ReMidterm(MRJob): 
    """ The goal of this mapreduce job is to use the data files in /dataset/orders
    to compute the total share for each word, and the total number of appearances
    of each word. Words appearing 200 or more times are excluded from the output. """

    def mapper(self, key, line):
        """ This mapper expects the following information as input:
          key: The mapper will emit the word as the key
          value: The mapper will emit the share for each word and an integer as the value         
          and its goal is to yield the word share and number of appearances for each word """
        orders_parts = line.split("\t") #split data by tab
        if orders_parts[0] == 'StockCode': #Skip header
            pass
        else:
            if len(orders_parts) == 3: #Items table
                description, unitprice = orders_parts[1].strip(), orders_parts[2].strip()
                description_split = description.split()
                word_share = len(description_split) #Share for each word comes from the description length
                for w in description_split:
                    word_share = float(unitprice) / word_share #Gets the price shared among word appearances
                    yield w, (word_share, 1)

    def combiner(self, key, value_list):
        """ This combiner expects the following information as input:
        key: The combiner will emit the word as the key
        value: The combiner will emit the total share and total appearances as the value
        and its goal is to yield the total share and total number of appearances for
        each word """
        total_appearances = 0 #Initializes
        total_share = 0 #Initializes

        for share, count in value_list:
            total_appearances += count #Adds the word count 
            if total_appearances < 200: #Keeps words appearing less than 200 times
                total_share += share #Updates total share
        yield key, (total_share, total_appearances)

    def reducer(self, key, value_list):
        """ This reducer expects the following information as input:
        key: The reducer will emit the word as the key
        value: The reducer will emit the total share and total number of appearances for each word as the value
        and its goal is to yield the total share and total number of appearances for each word """
        total_appearances = 0 #Initializes
        total_share = 0 #Initializes

        for share, count in value_list:
            total_appearances += count #Adds the word count
            if total_appearances < 200: #Keeps words appearing less than 200 times
                total_share += share #Updates total share
        yield key, (round(total_share, 4), total_appearances) #Yields total share rounded to 4 decimal digits

if __name__ == '__main__':
    ReMidterm.run()  
