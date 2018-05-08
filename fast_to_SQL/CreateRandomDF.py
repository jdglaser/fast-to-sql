# -*- coding: utf-8 -*-
"""
Created on Thu Apr 19 07:44:05 2018

@author: jglasej

This Function creates a random DF of length 'n', at random seed 'seed', with
all of the major datatypes as columns (Integer,Float,String,Date,Boolean).
This is great for testing stuff.

"""
from datetime import datetime
import random
import pandas as pd

def random_df(n=10000,seed=1234):
    inte = list(range(0,n))
    random.seed(12345)
    boo = []
    for i in range(0,n):
        x = random.randint(0,1)
        if x == 0:
            boo.append(False)
        elif x == 1:
            boo.append(True)
    dat = []
    for i in range(0,n):
        dat.append(datetime(random.randint(1990,2017),random.randint(1,12),random.randint(1,28)))
    
    flo = []
    for i in range(0,n):
        flo.append(random.random())
    
    first_names = ("Bob","Lucy","Tim","Joe","Harrold","Rachel","Matt","Jarred","Jessica","Lauren",
                   "WonderWoman","Superman","Darth","Vader","Jack","Ricky","Ross","Monica","Chandler",
                   "Zachary", "Mitchell", "Frankie", "Callum", "Byrne", "Dexter", "Jones", "Sam", "Henderson", 
                   "Quinn", "Snyder", "Kelvin", "Smith", "Bryson", "Mayer", "Kamden", "Patterson", "Jaeden", "Burks")
    last_names = ("Kieran", "Riley", "Edward", "Clark", "Joe", "Woods", "Leo", "Young", "Noah", "Graham", "Brent", "Carey",
                  "Joshua", "Nichols", "Kolten", "Sanders", "Pierce", "Wiley", "Sean", "Gregory", "Louie", "Mason",
                  "Joshua", "Richards", "James", "Harper", "Charles", "Sutton", "Camren", "Gutierrez", "Niko", "Byers", "Maximiliano", "Terry",
                  "Keegan", "Mcclure", "Vance", "Bentley")  
    
    cha = []
    for i in range(0,n):
        cha.append(random.choice(first_names)+" "+random.choice(last_names))
    
    nul = []
    for i in range(0,n):
        x = random.randint(0,1)
        if x == 0:
            nul.append("NULL")
        elif x == 1:
            nul.append(1)
            
    df = pd.DataFrame({"Inte":inte,"Cha":cha,"Boo":boo,"Dat":dat,"Flo":flo,"Nul":nul})
    return df

if __name__ == "__main__":
    print(random_df(n=10000,seed=1234))