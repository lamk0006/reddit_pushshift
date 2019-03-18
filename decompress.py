# -*- coding: utf-8 -*-
"""
Created on Thu Mar 14 01:08:09 2019

@author: Group 6 - Jonathan Lam Kam Cheung @ https://www.linkedin.com/in/jonathan-lam-kam-cheung/
Students of Algonquin College
Program: 19W_CST2200_300 Database Systems  Administration and Management (Foundations of transforming  and storing Big Data)
"""

import os
import bz2
import lzma
import gzip
import shutil
from pathlib import Path
import zstandard as zstd
import multiprocessing as mp

class decompress:
        
    def multiprocess_extaction_func(self,path_to_read_file):
         file_name = path_to_read_file[path_to_read_file.rfind("\\")+1:]
         file_name_extension = Path(file_name).suffix
         file_name_wo_extension = file_name[:file_name.rfind(file_name_extension)]
         path_to_save_file = self.path_to_save_file_df + file_name_wo_extension
         if file_name_extension == ".bz2":
             with bz2.BZ2File(path_to_read_file,"rb") as fr, open(path_to_save_file,"wb") as fw:
                 shutil.copyfileobj(fr,fw,length = 65536)
                 print(self.present_decompress_file + file_name_wo_extension)
         elif file_name_extension == ".xz":
             with lzma.open(path_to_read_file, 'rb') as fr, open(path_to_save_file, 'wb') as fw:
                 shutil.copyfileobj(fr, fw, 65536)
                 print(self.present_decompress_file + file_name_wo_extension)              
         elif file_name_extension == ".gz": 
             with gzip.open(path_to_read_file, 'rb') as fr, open(path_to_save_file, 'wb') as fw:
                 shutil.copyfileobj(fr, fw, 65536)
                 print(self.present_decompress_file + file_name_wo_extension)             
         elif file_name_extension == ".zst":
             dctx = zstd.ZstdDecompressor()
             with open(path_to_read_file, 'rb') as ifh, open(path_to_save_file, 'wb') as ofh:
                dctx.copy_stream(ifh, ofh, write_size=65536) 
                print(self.present_decompress_file + file_name_wo_extension)               
     
    def decompress_file(self,dir_to_read_file,prefix):
        self.present_decompress_file = 'Decompressed->         '  
        if prefix == 's':
            self.present_decompress_file = 'RS ' + self.present_decompress_file
            self.dtrf_df = dir_to_read_file + 'Step 1 - Data Lake\Reddit Submissions\\'
            self.path_to_save_file_df = dir_to_read_file + 'Step 2 - Decompress\Reddit Submissions\\'  
        if prefix == 'c':
            self.present_decompress_file = 'RC ' + self.present_decompress_file
            self.dtrf_df = dir_to_read_file + 'Step 1 - Data Lake\Reddit Comments\\'
            self.path_to_save_file_df = dir_to_read_file + 'Step 2 - Decompress\Reddit Comments\\'   
        if prefix == 'w':
            self.present_decompress_file = 'WK ' + self.present_decompress_file
            self.dtrf_df = dir_to_read_file + 'Step 1 - Data Lake\Wikipedia Pagecounts\\'
            self.path_to_save_file_df = dir_to_read_file + 'Step 2 - Decompress\Wikipedia Pagecounts\\'  
        extraction_list = []           
        directory = os.fsencode(self.dtrf_df)
        if not os.path.exists(os.path.dirname(self.path_to_save_file_df)):
            os.makedirs(os.path.dirname(self.path_to_save_file_df))                
        
        for file_to_uncompress in os.listdir(directory):
            extraction_list.append(directory.decode() + file_to_uncompress.decode())  

        extraction_list.sort()    
        with mp.Pool(mp.cpu_count()) as p:
            p.map(self.multiprocess_extaction_func, extraction_list)