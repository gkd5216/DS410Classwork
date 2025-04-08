from mrjob.job import MRJob

class WordCount(MRJob):
    """ The goal of this mapreduce job is to use the hdfs dataset in /datasets/retailtab 
    to compute, for each combination of country and month, the total amount spent and the 
    maximum price """

    def mapper(self, key, line):
        """ This mapper expects the following information as input:
          key: The mapper will emit the country and month as the key
          value: The mapper will emit the unitprice and quantity as the value           
          and its goal is to yield unit price and quantity for each combination 
          of country and month """
        words = line.split("\t") #Split words by tab
        if words[0] == 'InvoiceNo': #Skip header
            pass
        else:
            country = words[7].strip() #Extract country
            unitprice = float(words[5].strip()) #Extract unitprice
            quantity = int(words[3].strip()) #Extract quantity
            invoicedate = words[4].strip() #Extract date
            invoicedatesplit = invoicedate.split('/') #Split by '/' to get month
            month = invoicedatesplit[0] #First portion is the month
            yield (country, month), (unitprice * quantity, unitprice)
     
    def combiner(self, key, value_list):
        """ This combiner expects the following information as input:
        key: This combiner will emit the country and month as the key
        value: This combiner will emit the total amount spent and maximum price on items
        as the value
        and its goal is to yield the sum of total amount spent and compute the 
        maximum price for each country and month """
        total_amount_spent = 0 
        maximum_price = 0 
        for spent, price in value_list: 
            total_amount_spent += spent #Add calculation of total price
            if price > maximum_price: 
                maximum_price = price #Maximum price is the unit price if its greater
        yield key, (total_amount_spent, maximum_price)
        

    def reducer(self, key, value_list):
        """ This reducer expects the following information as input:
        key: The reducer will emit the combination of country and month as the key
        value: The reducer will emit the total amount spent on items and the 
        maximum price
        and its goal is to yield the total amount spent and the maximum price for 
        each combination of country and month """
        total_amount_spent = 0 
        maximum_price = 0 
        for spent, price in value_list: #Loop through list of values
            total_amount_spent += spent
            if price > maximum_price:
                maximum_price = price
        yield key, (round(total_amount_spent, 2), round(maximum_price, 2))

if __name__ == '__main__':
    WordCount.run() #MRJob version

### Bugs detected and fixed ###
# 1. Combiner changed the output significantly; Changed combiner code with code from MrLab2
# 2. Reducer not adding up total amount correctly; Changed variable names in the list of values in reducer
# 3. Total amount being outputted not right; rounded the total amount spent to 2 digits in the reducer
# 4. Maximum price not being outputted right; rounded the maximum price to 2 digits in the reducer
