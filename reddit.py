# -*- coding: utf-8 -*-
"""
Created on Sun Mar  3 23:20:10 2019

@author: Group 6 - Jonathan Lam Kam Cheung @ https://www.linkedin.com/in/jonathan-lam-kam-cheung/
Students of Algonquin College
Program: 19W_CST2200_300 Database Systems  Administration and Management (Foundations of transforming  and storing Big Data)
"""

import os
import time
import sys
import fnmatch
import requests
import urllib.request
from bs4 import BeautifulSoup
from multiprocessing.dummy import Pool as ThreadPool 

class reddit:
    
    def multithread_download_files_func(self,list_of_file):
        filename = list_of_file[list_of_file.rfind("/")+1:]
        path_to_save_filename = self.ptsf_download_files + filename
        if not os.path.exists(path_to_save_filename): 
            data_content = None
            try:
                request = urllib.request.Request(list_of_file)
                response = urllib.request.urlopen(request)
                data_content = response.read()
            except urllib.error.HTTPError:
                retries = 1
                success = False
                while not success:
                    try:
                        response = urllib.request.urlopen(list_of_file)
                        data_content = response.read()                        
                        success = True
                    except Exception:
                        wait = retries * 15;
                        print('Service Temporarily Unavailable! Retrying in ' + str(wait) + 's on file: ' + filename)
                        sys.stdout.flush()                          
                        time.sleep(wait)
                        retries += 1               
            if data_content:
                with open(path_to_save_filename, 'wb') as wf:    
                    wf.write(data_content)                 
                    print(self.present_download_files + filename)                        
        
    def download_files(self,filter_files_df,url_to_download_df,path_to_save_file_df,prefix):
        if filter_files_df == '*':
            reddit_years = [2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018]
            filter_files_df = ''
            for idx, ry in enumerate(reddit_years):
                filter_files_df += '*' + str(ry) + '*'
                if (idx != len(reddit_years)-1):
                    filter_files_df += '&'   
                    
        download_filter = list([x.strip() for x in filter_files_df.split('&')])
        download_filter.sort()
        
        keyword_filter = ''
        self.present_download_files = 'Downloaded->           '
        if prefix == 's':
            self.ptsf_download_files = path_to_save_file_df + 'Step 1 - Data Lake\Reddit Submissions\\'
            keyword_filter = 'rs'
            self.present_download_files = 'RS ' + self.present_download_files
            #Include 1 previous year for submissions only
            #min_year = download_filter[0] 
            #int_min_year = int(min_year[1:-1])-1 #minus 1 year            
            #download_filter.append('*' + str(int_min_year) + '*')  
            #download_filter.sort()
        
        if prefix == 'c':
            self.ptsf_download_files = path_to_save_file_df + 'Step 1 - Data Lake\Reddit Comments\\'        
            keyword_filter = 'rc'
            self.present_download_files = 'RC ' + self.present_download_files
        
        #If folder doesn't exist, create one
        if not os.path.exists(os.path.dirname(self.ptsf_download_files)):
            os.makedirs(os.path.dirname(self.ptsf_download_files))        
        
        r_df  = requests.get(url_to_download_df)
        data_df = r_df.text
        soup_df = BeautifulSoup(data_df,features="lxml")
        
        #Get the href links from the website
        list_of_href_df = set()
        for link_df in soup_df.find_all('a'):
            href_link_df = link_df.get('href')
            list_of_href_df.add(url_to_download_df + href_link_df[2:])

        #Clean from external href links     
        clean_list_of_href_df = []
        for loh_df in list_of_href_df:
            if keyword_filter in loh_df.lower():
                clean_list_of_href_df.append(loh_df)
        
        #Filter the links
        matching_fnmatch_list = []
        if filter_files_df != '':
            if prefix == '*':
                pass
            for dfilter in download_filter:
                fnmatch_list = fnmatch.filter(clean_list_of_href_df, dfilter) 
                i = 0
                for fnl in fnmatch_list:
                    '''temporary break to test'''
                    if i == 2:
                        break
                    i += 1
                    matching_fnmatch_list.append(fnl)
                    
        if not matching_fnmatch_list:
            matching_fnmatch_list = clean_list_of_href_df.sort()
        matching_fnmatch_list.sort()
            
        p = ThreadPool(200)
        p.map(self.multithread_download_files_func, matching_fnmatch_list)