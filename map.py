#!/usr/bin/env python2
#-*- coding:utf-8 -*-
import pyreducer
import os
import logging

def Raw_data_maker(path):
    """把某个目录下全部文件的记录加入datasourse"""
    ilist=os.listdir(path)
    datasourse = []
    for item in ilist:
        itemsrc = os.path.join(path, item)
        File = open(itemsrc)
        while 1:
            lines = File.readlines(30000)
            if not lines:
                break
            for line in lines:
	        if line[87:92] !='9999':
                     datasourse.append(line)
        File.close()
    datadict = dict(enumerate(datasourse))
    return datadict

def mapfn(key,value):
    """返回年月和温度"""
    date = value[15:21]  #取出年月
    temp = value[87:92] #取出温度
    temperature = int(temp)
    return {date:temperature}.iteritems()

def reducefn(k,vset):
     aver = sum(vset) // (len(vset)*10)
     return aver
def resultfn(result):
    print result
    weatherdict = sorted(result.iteritems(),key = lambda d:d[0])
    f = open("weather.txt",'w')
    header1 = "These data are handled and sorted\n"
    header2 = "******Powered by mapreduce****** \n\n"
    f.writelines(header1+header2)
    for item in weatherdict:
        stri = "Time:%s  Average Temperature:%d\n" % item
        if item[0][4:] =='01':
            line = '***'*10
            stri = line +'\n' + stri 
        f.writelines(stri)
    f.close()
    print 'All data has been successfully handled !'
         
      
def main():
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    path='/home/richard/code/mapreduce/testmap'
    datadic = Raw_data_maker(path)
    ser= pyreducer.Server("", 20000)
    ser.mapfn= mapfn
    ser.reducefn=reducefn
    ser.resultfn = resultfn
    
    ser.source=datadic
    ser.get_tasks()
    ser.run_server()
    

if __name__=="__main__":
    main()



