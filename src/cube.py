"""
c = Cube()

c.add({'city':'New York', 'product':'Pv1', 'year':2008}, 1000)
c.add({'city':'Atlanta', 'product':'Pv1', 'year':2008}, 800)
c.add({'city':'San Francisco', 'product':'Pv1', 'year':2008}, 950)

c.add({'city':'New York', 'product':'Pv1', 'year':2009}, 900)
c.add({'city':'Atlanta', 'product':'Pv1', 'year':2009}, 1150)
c.add({'city':'San Francisco', 'product':'Pv1', 'year':2009}, 725)

# in 2009 rolled out a new product "Pv2"
c.add({'city':'New York', 'product':'Pv2', 'year':2009}, 500)
c.add({'city':'Atlanta', 'product':'Pv2', 'year':2009}, 275)
c.add({'city':'San Francisco', 'product':'Pv2', 'year':2009}, 400)


# summarize data along city dimension 
results = c.query(groups=['city'])

print results.values['Atlanta'].get_data()
>>[800, 1150, 275]

print results.values['Atlanta'].get_data(aggregator=sum)
>> 2225

# summarize data along city dimension but filter only data from 2009
results = c.query(attributes={'year':2009}, groups=['city'])

print results.values['Atlanta'].get_data(aggregator=sum)
>> 1425
"""


class Dimension():
    
    def __init__(self):
        
        self.values = {}
        
    def add(self, value, id):
        
        if not self.values.has_key(value):
            self.values[value]  = set()
        
        self.values[value].add(id)
    
    def get(self, value):
        
        if not self.values.has_key(value):
            return set()
        
        return self.values[value]
     
     

class Result:
    
    def __init__(self, cube, dataset, groups=[]):
        
        self.cube = cube
        self.dataset = dataset
        self.group = False
        
        if groups:
        
            self.group = groups.pop(0)
            self.values = {}
            
            for value in cube.dimensions[self.group].values:
                
                subgroups = list(groups)
                
                group_ids = dataset.intersection(self.cube.dimensions[self.group].get(value))
                
                self.values[value] = Result(self.cube, group_ids, subgroups)
                
                
    
    def get_data(self, aggregator=None):
    
        data = self.cube.get_data(self.dataset)
    
        if aggregator:
            
            if data:
                return aggregator(data)
            else:
                return 0
            
        else:
            
            return data
        
    def to_string(self, tabs=''):
        
        string = '' 
        
        if self.group:
            string += tabs + str(self.group) + ':\n'
            
            tabs += '\t'
            
            for value in self.values:
                
                data = self.values[value].get_data()
                
                if data:
                    string += tabs + str(value) + ':\n'
                    
                    string += tabs + str(data) + '\n'
                    
                    string += self.values[value].to_string(tabs) + '\n'
                    
        else:
            string = tabs + str(self.get_data()) + '\n'
            
        
        return string 
    
    def __str__(self):
        
        return self.to_string()
        
        
     
    
class Cube:
    
    def __init__(self):
        
        self.dimensions = {}
        self.data = []
        self.ids = set()
        

    def add(self, atttributes, data):
        
        id = len(self.data)
        
        self.data.append(data)
        self.ids.add(id)
        
        for atttribute in atttributes:
            
            if not self.dimensions.has_key(atttribute):
                self.dimensions[atttribute] = Dimension()
                    
            self.dimensions[atttribute].add(atttributes[atttribute], id)
            
    def query(self, attributes={}, groups=[]):

        if len(attributes):
            
            final_set = None
            
            for atttribute in attributes:
                
                if not final_set:
                    final_set = self.dimensions[atttribute].get(attributes[atttribute])
                else:
                    final_set = final_set.intersection(self.dimensions[atttribute].get(attributes[atttribute]))
        else:
            final_set = self.ids
        
        return Result(self, final_set, groups)
        
        
    def get_data(self, dataset):
        
        slice = []
        
        for id in dataset:
            
            slice.append(self.data[id])
            
        return slice 
            
            
#c = Cube()
#
#c.add({'catA':1,'catB':1,'catC':1}, {'a':1})
#c.add({'catA':2,'catB':1,'catC':1}, {'a':5})
#c.add({'catA':2,'catB':2,'catC':1}, {'a':3})
#c.add({'catA':2,'catB':2,'catC':2}, {'a':4})
#c.add({'catA':2,'catB':2,'catC':3}, {'a':5})
#
#q = c.query(groups=['catA', 'catB'])
#
#print q


