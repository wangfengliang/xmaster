# -*- coding: utf-8 -*-
 
def list_all_members(obj):
    for name,value in vars(obj).items():
        yield (name, value)

def list_pb_all_numbers(pb_obj):
    if not pb_obj:
        yield StopIteration()
    for field, value in pb_obj._fields.iteritems(): 
        yield (field.name, value)

if __name__== '__main__':

    class Site(object):
        def __init__(self):
            self.title = 'share js code'
            self.url = 'http://www.sharejs.com'
            self.n = 1
          
    site = Site()
    for (name, value) in list_all_members(site):
        print('%s=%s  %s %s'%(name, value, type(name), type(value)))

    
