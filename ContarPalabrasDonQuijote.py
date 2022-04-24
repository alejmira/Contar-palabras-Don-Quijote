#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 23 18:58:00 2022

@author: alejandro
"""

import sys
import random
import pyspark

def main(file, percentage):
    with pyspark.SparkContext() as sc:
        # Cargamos fichero, filtramos lineas aleatorias y las guardamos
        lines = sc.textFile(file)
        random_lines = lines.filter(lambda x: random.randint(0, 100) < percentage)
        random_lines.saveAsTextFile("quijote_s05.txt")   
        
        # Contamos palabras del fichero de líneas aleatorias
        random_lines_split = random_lines.map(lambda x: x.split())
        random_words = sc.parallelize(random_lines_split.reduce(lambda x, y: x + y))
        random_words_weighted = random_words.map(lambda x: (x, 1))
        random_words_count = random_words_weighted.reduceByKey(lambda x, y: x + y)
        random_words_count.saveAsTextFile("out_quijote_s05.txt")
        
        # Contamos palabras del fichero original
        lines_split = lines.map(lambda x: x.split())
        words = sc.parallelize(lines_split.reduce(lambda x, y: x + y))
        words_weighted = words.map(lambda x: (x, 1))
        words_count = words_weighted.reduceByKey(lambda x, y: x + y)
        words_count.saveAsTextFile("out_quijote.txt")
        
        
        
if __name__ == "__main__":
    if len(sys.argv) == 1:
        main("Don Quijote de La Mancha", 5)
    elif len(sys.argv) == 2:
        main(sys.argv[1], 5)
    elif len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])
    
    
    
    