from mrjob.job import MRJob
class TotalShareAppearances(MRJob):
    """ The goal of this mapreduce job is to use the orders dataset in /datasets/orders to compute the total share for each word, and the total number of appearances of each word with word appearances of 200 or more eliminated from output """
    def mapper(self, key, line):
        """ This mapper expects the following information as input:
        key: The mapper will emit the item description as the key
        value: The mapper will emit the unitprice and an integer 1 from
        the items table as the value
        and its goal is to yield the unitprice and integer 1 for each description """
        orders_parts = line.split("\t") #Split by tab
        if orders_parts[0] == "StockCode" or orders_parts[0] == "InvoiceNo": #Skip header
            pass
        else:
            if len(orders_parts) == 3: #Items Table
                description = orders_parts[1].strip().split(" ")
                unitprice = orders_parts[2].strip()
                stockcode = orders_parts[0].strip()
                yield description, (unitprice, 1)
    
    def combiner(self, key, value_list):
        total_share = 0
        appearance_count = 0
        unitprice = None
        for value in value_list:
            if value[1] == 1:
                unitprice = value[0]
                appearance_count += 1
                if appearance_count < 200:
                    unitprice = unitprice

        if unitprice is not None:
            yield key, (unitprice, appearance_count)

    def reducer(self, key, value_list):
        """ This reducer expects the following information as input:
        key: The reducer will emit the word from the description as the key
        value: The reducer will emit the total share and appearance count as 
        the value
        and its goal is to compute the total share and sum up the total number 
        of appearances for each word based on the value type """
        
        total_share = 0 #Initialize
        appearance_count = 0 #Initialize
        unitprice = None
        
        for value in value_list:
            if value[1] == 1:
                unitprice = value[0]
                appearance_count += 1
                if appearance_count < 200:
                    unitprice = unitprice

        if unitprice is not None:
            yield key, (unitprice, appearance_count)

if __name__ == '__main__':
    TotalShareAppearances.run()
