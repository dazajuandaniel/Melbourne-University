# -*- coding: utf-8 -*-
import os,ijson,json
import numpy as np
from mpi4py import MPI
import timeit
start_time = timeit.default_timer()

#Function Definition
def find_coords(line_str):
    find_string='coordinates":['
    first_ch=line_str.find(find_string)
    if first_ch==-1:
        return -999
    dist=len(find_string)
    comma=line_str.find(",",first_ch+1)
    sec_paren=line_str.find("]",comma+1)
    x_coor=line_str[first_ch+dist:comma]
    y_coor=line_str[comma+1:sec_paren]
    try:
        x=float(x_coor.strip("'"))
        y=float(y_coor.strip("'"))
    except:
        return -999
    return x,y 

def open_file_string(filename):
    coord_list=[]
    f = open(filename,"r")
    for index,line in enumerate(f):
        result=find_coords(line)
        if result==-999:
            continue      
        else:
            if type(result[0])==type(1.0) and type(result[1])==type(1.0):
                coord_list.append(result[0])
                coord_list.append(result[1])
    return coord_list

def open_file_ijson(filename):
    coord_list=[]
    f = open(filename,"r")
    objects = ijson.items(f,'item.json.coordinates.coordinates')
    for it in objects:
        coord_list.append(it[0])
        coord_list.append(it[1])
    #return np.array(coord_list)
    return coord_list

def update_polygon(x,y):
    res={}
    #Check Column first
    if x<144.7 or x>=145.45:
        return 'outside'
    elif x<=144.85:
        res['column']=1
    elif x<=145:
        res['column']=2
    elif x<=145.15:
        res['column']=3
    elif x<=145.3:
        res['column']=4
    elif x<=145.45:
        res['column']=5
    else:
        return 'outside'
    #Check Rows
    if y>-37.5 or y<-38.1:
        return 'outside'
    elif y>=-37.65:
        if res['column']==5:
            return 'outside'
        res['row']='A'
    elif y>=-37.8:
        if res['column']==5:
            return 'outside'
        res['row']='B'
    elif y>=-37.95:
        res['row']='C'
    elif y>=-38.1:
        if (res['column']==1 or res['column']==2):
            return 'outside'
        res['row']='D'
    else:
        return 'outside'
    res['gridpos']=res['row']+str(res['column'])
    return res 

def count_tweets_grid2(coordinates):
    count=0
    res=[]
    #print comm.Get_rank()
    dic_grid={'A':0,'B':0,'C':0,'D':0,
              1:0,2:0,3:0,4:0,5:0,
             'A1':0,'A2':0,'A3':0,'A4':0,
             'B1':0,'B2':0,'B3':0,'B4':0,
             'C1':0,'C2':0,'C3':0,'C4':0,'C5':0,
             'D3':0,'D4':0,'D5':0}
    for index in range(len(coordinates)):
    #for index,item in enumerate(coordinates):
        if index==0:
            x=coordinates[0]
            y=coordinates[1]
        elif index==1:
            continue
        elif index==len(coordinates)-1:
            break
        elif index%2==0:
            x=coordinates[index]
            y=coordinates[index+1]
            index=index+1
        else:
            x=coordinates[index]
            y=coordinates[index+1]
            index=index+1
        ans=update_polygon(x,y)

        if ans !='outside':
            dic_grid[ans['column']]+=1
            dic_grid[ans['row']]+=1
            dic_grid[ans['gridpos']]+=1
    res=dict_to_list(dic_grid)
    return res

def dict_to_list(dicts):
    arr_res=[]
    for i in sorted(dicts.items()):
        arr_res.append(i[1])
    return arr_res

def print_final():
    return ["Col 1","Col 2","Col 3","Col 4","Col 5",
    "Row A","A1","A2","A3","A4",
    "Row B","B1","B2","B3","B4",
    "Row C","C1","C2","C3","C4","C5",
    "Row D","D3","D4","D5"]    
#MPI Basics
comm = MPI.COMM_WORLD
size=comm.Get_size()
rank = comm.Get_rank()
if rank==0:
    tinyTwitter="tinyTwitter.json"
    smallTwitter="smallTwitter.json"
    bigTwitter="bigTwitter.json"
    tweet_coordinates=open_file_string(bigTwitter)
    #tweet_coordinates=open_file_ijson(smallTwitter)
    n=len(tweet_coordinates)
    steps=n/size
    for i in range(1,size):
        start=i*steps
        end=i*steps+(steps-1)
        comm.send(tweet_coordinates[start:end],dest=i,tag=i)

    local_data=tweet_coordinates[0:steps-1]
    total=np.ndarray(shape=(size,25))

else:
    local_data=comm.recv(source=0,tag=rank)
    total=None

local_res=count_tweets_grid2(local_data)
#print "Rank "+str(rank)+" calculated ",local_res

if rank==0:
    aggregate=np.zeros(25)
    total[0]=local_res
    strings_vec=print_final()
    for i in range(1,size):
        total[i]=comm.recv(source=i,tag=i)
    for i in range(0,25):
        for j in range(0,size):
            aggregate[i]=aggregate[i]+total[j,i]
    
    #Final Print
    grid_dict={}
    column_dict={}
    row_dict={}
    for i in range(5):
        grid_dict[strings_vec[i]]=aggregate[i]
    column_dict[strings_vec[5]]=aggregate[5]
    column_dict[strings_vec[10]]=aggregate[10]
    column_dict[strings_vec[15]]=aggregate[15]
    column_dict[strings_vec[21]]=aggregate[21]
    for i in range(25):
        if i<=5 or i==10 or i==15 or i==21:
            continue
        else:
            row_dict[strings_vec[i]]=aggregate[i]

    print "By Grid: \n"
    for key, value in sorted(grid_dict.iteritems(), key=lambda (k,v): (v,k), reverse=True):
        print "%s: %s" % (key, value)
    print "By Columns: \n"
    for key, value in sorted(column_dict.iteritems(), key=lambda (k,v): (v,k), reverse=True):
        print "%s: %s" % (key, value)
    print "By Rows: \n"
    for key, value in sorted(row_dict.iteritems(), key=lambda (k,v): (v,k), reverse=True):
        print "%s: %s" % (key, value)

    #for i in range(5):
    #    resul_dict[strings_vec[i]]=aggregate[i]
    #print resul_dict
    #for i in range(25):
    #    print strings_vec[i],": ",aggregate[i]
    
        
else:
    comm.send(local_res,dest=0,tag=rank)
elapsed = timeit.default_timer() - start_time
print "Total Time: ",elapsed