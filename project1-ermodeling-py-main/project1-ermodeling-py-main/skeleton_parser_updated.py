#!/usr/bin/env python
# coding: utf-8

# In[ ]:



"""
FILE: runparser_updated.py
------------------
Author: Hadi Hashemi Nejad (mhashemineja@wisc.edu)
Modified: 10/10/2021

Skeleton parser for CS564 programming project 1. Has useful imports and
functions for parsing, including:

1) Directory handling -- the parser takes a list of eBay json files
and opens each file inside of a loop. You just need to fill in the rest.
2) Dollar value conversions -- the json files store dollar value amounts in
a string like $3,453.23 -- we provide a function to convert it to a string
like XXXXX.xx.
3) Date/time conversions -- the json files store dates/ times in the form
Mon-DD-YY HH:MM:SS -- we wrote a function (transformDttm) that converts to the
for YYYY-MM-DD HH:MM:SS, which will sort chronologically in SQL.

Your job is to implement the parseJson function, which is invoked on each file by
the main function. We create the initial Python dictionary object of items for
you; the rest is up to you!
Happy parsing!
"""

import sys
from json import loads
from re import sub
import pickle 

columnSeparator = "|"

# Dictionary of months used for date transformation
MONTHS = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',        'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}

"""
Returns true if a file ends in .json
"""
def isJson(f):
    return len(f) > 5 and f[-5:] == '.json'

"""
Converts month to a number, e.g. 'Dec' to '12'
"""
def transformMonth(mon):
    if mon in MONTHS:
        return MONTHS[mon]
    else:
        return mon

"""
Transforms a timestamp from Mon-DD-YY HH:MM:SS to YYYY-MM-DD HH:MM:SS
"""
def transformDttm(dttm):
    dttm = dttm.strip().split(' ')
    dt = dttm[0].split('-')
    date = '20' + dt[2] + '-'
    date += transformMonth(dt[0]) + '-' + dt[1]
    return date + ' ' + dttm[1]

"""
Transform a dollar value amount from a string like $3,453.23 to XXXXX.xx
"""

def transformDollar(money):
    if money == None or len(money) == 0:
        return money
    return sub(r'[^\d.]', '', money)

"""
Transform a quotation in the description attribute to have quotations around it
"""

def quotation_editor(description):
    initial_length = len(description)
    new_description = ""
    for i in range(initial_length):
        if description[i] == "\"":
            new_description += "\"" + description[i] + "\""
        else:
            new_description += description[i]
    return new_description


"""
Parses a single json file. Currently, there's a loop that iterates over each
item in the data set. Your job is to extend this functionality to create all
of the necessary SQL tables for your database.
"""
def parseJson(json_file):
    with open(json_file, 'r') as f:
        items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file
        Items_string = ""
        User_string = ""
        Category_string = ""
        Bids_on_string = ""
        Items_columns=["ItemID","Name","Started","Ends","Location","Number_of_Bids","First_Bid","Buy_Price","Country","Description","Currently"]
        for item in items:
            for i in range(len(items)):
                #Makes Items table
                for col_name in Items_columns:
                    if col_name not in items[i]:
                        Items_string += "NULL" + columnSeparator
                    else:
                        if col_name == "Started" or col_name == "Ends":
                            Items_string += str(transformDttm(items[i][col_name])) + columnSeparator
                        elif col_name == "Currently" or col_name == "First_Bid":
                            Items_string += str(transformDollar(items[i][col_name])) + columnSeparator
                        elif col_name == "Description":
                            Items_string == str(quotation_editor(items[i][col_name])) + columnSeparator
                        else:
                            Items_string += str(items[i][col_name]) + columnSeparator
                Items_string += "\n"

                #Makes User table and Bids_On table
                if items[i]["Bids"] != None:
                    for j in range(len(items[i]["Bids"])):
                        for user_col in items[i]["Bids"][j]["Bid"]["Bidder"]:
                            if user_col == "Amount":
                                User_string += str(transformDollar(items[i]["Bids"][j]["Bid"]["Bidder"][user_col])) + columnSeparator
                            else:
                                User_string += str(items[i]["Bids"][j]["Bid"]["Bidder"][user_col]) + columnSeparator
                        User_string += "\n"
                    Bids_on_string += str(transformDttm(items[i]["Bids"][j]["Bid"]["Time"])) + columnSeparator
                    Bids_on_string += str(items[i]["Bids"][j]["Bid"]["Amount"]) + columnSeparator + "\n"
                User_string += str(items[i]["Seller"]["UserID"]) + columnSeparator
                User_string += str(items[i]["Seller"]["Rating"]) + columnSeparator
                User_string += "Null|Null|\n"

                #Makes Category table
                
                for j in items[i]["Category"]:
                    Category_string += str(items[i]["ItemID"]) + columnSeparator
                    Category_string += str(items[i]["Category"][j]) + "\n"

                x = open("items_table", "wb")
                pickle.dump(Items_string, x)
                
                y = open("user_table", "wb")
                pickle.dump(User_string, y)

                z = open("category_table", "wb")
                pickle.dump(Category_string, z)
                
                h = open("bids_on_table", "wb")
                pickle.dump(Bids_on_string, h)
                
def main(argv):
    if len(argv) < 2:
        print >> sys.stderr, 'Usage: python skeleton_json_parser.py <path to json files>'
        sys.exit(1)
    # loops over all .json files in the argument
    for f in argv[1:]:
        if isJson(f):
            parseJson(f)
            print ("Success parsing ") + f

if __name__ == '__main__':
    main(sys.argv)


