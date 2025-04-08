from mrjob.job import MRJob

class WordCount(MRJob):
    def mapper(self, key, line):
        words = line.split("\t") #Split words by tab
        if words[0] == 'InvoiceNo':
            pass
        else:
            country = words[7].strip() #Extract country
            unitprice = float(words[5].strip()) #Extract unitprice
            quantity = int(words[3].strip()) #Extract quantity
            invoicedate = words[4].strip() #Extract date
            invoicedatesplit = invoicedate.split('/') #Split by '/' to get month
            month = invoicedatesplit[0] #First portion is the month
            yield((country, month), (unitprice, quantity))
    
    def reducer(self, key, values):
        total_amount_spent = 0 #Variable for total price
        maximum_price = 0 #Variable for maximum price
        for unitprice, quantity in values: #Loop through list of values
            total_amount_spent += unitprice * quantity #Add calculation of total price
            if unitprice > maximum_price: #Check if the maximum price is the unit price
                maximum_price = unitprice
        yield key, (total_amount_spent, maximum_price)

if __name__ == '__main__':
    WordCount.run() #MRJob version
