# This is an XML file reader. 

import os
import pandas as pd
from bs4 import BeautifulSoup
import xml.dom.minidom
from xml.etree import ElementTree
import lxml.etree as etree

# This is an XML file reader. 

#path = "/Users/thierryletendre/Desktop/Borealis/Python Code"
path = '' # Insert path here
os.chdir(path)

with open("EPA_octobre_2024-1-ddi.xml", "r") as f:
    xml_data = f.read()
xml_format = BeautifulSoup(xml_data, "xml")

"""
def read_format_xml():
    dom = xml.dom.minidom.parse("EPA_octobre_2024-1-ddi.xml")
    pretty_xml = dom.toprettyxml()
    print(pretty_xml)
read_format_xml()
"""

# THIS CODE IS A PROOF OF CONCEPT - NOT A MASTERPIECE

# It was specifically written with the French Labour Force Survey in mind.

# Because this is a proof of concept, the present code only extracts labels, 
# and frequency statistics from the DDI XML file. It wouldn't be too hard to
# edit the code in such a way that also extracts other metrics of importance.

# Running this function organises the labels and associated frequency stats
# in nested dictionaries.

def organised_groups():
    labl_xml = xml_format.find_all("labl")
    catstat_xml = xml_format.find_all("catStat")
    
    #print(catstat_xml)
    #print(labl_xml)
    
    count = 0

    new_list = []
    cat_list = []
    popped_list = []
    
    group_level = []
    category_level = []
    category_level_2 = []
    
    variable_level = []
    variable_level_2 = []


    for i in catstat_xml:
        if "<catStat type=" in str(i):
            f = str(i)
            f2 = f.removesuffix('</catStat>')
            f3 = f2.removeprefix('<catStat type="freq">')
            cat_list.append(f3)         
            

    for i in labl_xml:
        if "<labl>" in str(i):
            new_list = []
            f = str(i)
            f2 = f.removesuffix('</labl>')
            f3 = f2.removeprefix('<labl>')
            group_level.append(f3)            
            

        elif '<labl level="variable">' in str(i):
            if count == 0:
                if new_list != []:
                    category_level_2.append(new_list)
                    count += 1 
                    new_list = []
                    f = str(i)
                    f2 = f.removesuffix('</labl>')
                    f3 = f2.removeprefix('<labl level="variable">')
                    variable_level.append(f3)
                    new_list.append(f3)
                    
                
                elif new_list == []:
                    count += 1 
                    new_list = []
                    f = str(i)
                    f2 = f.removesuffix('</labl>')
                    f3 = f2.removeprefix('<labl level="variable">')
                    variable_level.append(f3)
                    new_list.append(f3)                    
                
            else:
                
                f = str(i)
                f2 = f.removesuffix('</labl>')
                f3 = f2.removeprefix('<labl level="variable">')
                variable_level.append(f3)
                new_list.append(f3)
    
    
        elif '<labl level="category">' in str(i):
            if count != 0:
                if new_list != []:
                    variable_level_2.append(new_list)
                    count = 0
                    new_list = []
                    f = str(i)
                    f2 = f.removesuffix('</labl>')
                    f3 = f2.removeprefix('<labl level="category">')
                    category_level.append(f3)
                    new_list.append(f3)
                    
                
                elif new_list == []:
                    count = 0
                    f = str(i)
                    f2 = f.removesuffix('</labl>')
                    f3 = f2.removeprefix('<labl level="category">')
                    category_level.append(f3)
                    new_list.append(f3)                    
                
            else:
                
                f = str(i)
                f2 = f.removesuffix('</labl>')
                f3 = f2.removeprefix('<labl level="category">')
                category_level.append(f3)
                new_list.append(f3)
    
    
    popped_list.append(variable_level.pop(0))
    popped_list.append(variable_level.pop(0))
    
    #print(popped_list)
    
    keys = category_level
    values = cat_list

    d = {k: v for k, v in zip(keys, values)}
    print(d)    
    
    

    #print(group_level)
    #print(variable_level)
    #print(len(keys))
    #print(category_level_2)
    #print(values)


# QUÉBEC HAS TO BE MANUALLY CHECKED TO CONFIRM VALUES - This was a bit of an oversight
# Everything is created and added to a dictionary of dictionaries (nested dictionaries)
# Issue is: there are 2 Québec (the province and the city) - as such, when there is a 
# Dictionary request, it always pulls the city instead of the province (the keys are the same).
# Code could easily be adapted and corrected for this. 
 
    
    new_dictionary = {}
    master_dictionary = {}
    
    key_counter_1 = 0
    key_counter_2 = 0
    key_counter_3 = 0
    
    while key_counter_1 != (len(keys)):
        if category_level_2[key_counter_2][key_counter_3] in d:
            new_dictionary[f'{category_level_2[key_counter_2][key_counter_3]}'] = f"{d[category_level_2[key_counter_2][key_counter_3]]}"
            key_counter_3 += 1
            key_counter_1 += 1
        
        if key_counter_3 >= len(category_level_2[key_counter_2]):
            master_dictionary[f'{variable_level[key_counter_2]}'] = new_dictionary
            new_dictionary = {}
            key_counter_2 += 1
            key_counter_3 = 0
    
    print(master_dictionary)
    
    dictionary_counter = 0
    for i in master_dictionary:
        df = pd.DataFrame.from_dict(master_dictionary[f"{i}"], orient = 'index')
        df.columns = [f'{i} (Nombre de réponses)']
        
        print("\n")
        #print(df)
        print(df.to_string())
   
   
# Originally, I meant to convert everything to excel sheets, but for some reason 
# I kept getting glitches. I'm sure there's an easy fix, but this code was written
# To illustrate what I was trying to do. So in the end I just ended up converting
# the shell output into a txt. file to test feasibility. 
        
"""        
        with pd.ExcelWriter("/Users/thierryletendre/Desktop/Borealis/Python Code/TestBook1.xlsx", mode = "a", engine = "openpyxl") as writer:
            df.to_excel(writer, sheet_name = f"{i}")     
"""



organised_groups()  
