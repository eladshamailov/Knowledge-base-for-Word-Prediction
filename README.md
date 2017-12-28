# Assignment2
 knowledge-base for Hebrew word-prediction system, based on Google 3-Gram Hebrew dataset, using Amazon Elastic Map-Reduce (EMR).
 
## Introduction
We will generate a knowledge-base for Hebrew word-prediction system, based on Google 3-Gram Hebrew dataset, using Amazon Elastic Map-Reduce (EMR). The produced knowledge-base indicates for a each pair of words the probability of their possible next words. In addition, We will  examine the quality of the algorithm according to statistic measures and manual analysis.

## Goal
Our goal is to  build a map-reduce system for calculating the conditional probability of each trigram (w1,w2,w3) found in the corpus:  Hebrew 3-Gram dataset of [Google Books Ngrams](https://aws.amazon.com/datasets/google-books-ngrams/).

The output of the system is a list of word trigrams (w1,w2,w3) and their conditional probabilities (P(w3|w1,w2))).
The list will be ordered: (1) by w1w2, ascending; (2) by the probability for w3, descending.

For example:

קפה נמס עלית 0.6
קפה נמס מגורען 0.4

קפה שחור חזק 0.6

קפה שחור טעים 0.3

קפה שחור חם 0.1

…

שולחן עבודה ירוק 0.7

שולחן עבודה מעץ 0.3

…

## Team
Elad Shamailov 308540202
Mor Bitan 305537383

## Instruction
Have Step1.jar - Step6.jar and Main.jar existing on the s3 bucket : assignment2dspmor .
If all the jars exists , run the Main.jar

## Description
Our application has 6 steps and a Main:

### Step1
Step1 takes as input the 1-gram corpus and parses it line by line.
For each line from the 1-gram corpus , it creates a line with the word and its occurrence , and another line with  * and the same occurrence , and sends it to the reducer.
In the reducer , we combine all the occurences by key. Note that we also sum up all the occurrences with * in order to get the sum of all the words occurences.

### Step2
Step2 takes as input the 2-gram corpus and parses it line by line.
For each line from the 2-gram corpus , it creates a line with the 2 words and their occurrence and sends it to the reducer.
In the reducer , we combine all the occurences by key.

### Step3
Step3 takes as input the 3-gram corpus and parses it line by line.
For each line from the 3-gram corpus , it creates a line with the 3 words and their occurrence and sends it to the reducer.
In the reducer , we combine all the occurences by key.

### Step4
Step4 takes as input Step2 and Step3 output.
If it's the output of Step2, it's a pair and the key will be the pair and the value is the occurence.
If it's the output of Step3 , it's 3 words , so we will split it into 2 pairs.
The first pair is the first and second words. the key is the pair and the value is the 3 words and their occurence.
The second pair is the second and third words. the key is the pair and the value is the 3 words and their occurence.
All this information is sent to the reducer.
In the reducer , the 3 words will become the key , and the value will be the pair and it's occurence.

### Step5
Step5 takes as input Step3 and Step4 output.
If it's the output of Step3, the key is the 3 words and the value is the occurence.
If it's the output of Step4 , the key is the 3 words and the value is the pair and it's occurence.
All the information is sent to the reducer.
In the reducer , we start with the setup function. The setup function loads from the hdfs all Step1 output.
After this , we calculate the probability of the apperance of 3 words in the text.
The way to calculate it is:

<p>
  <img src="https://github.com/eladshamailov/Assignment2/blob/master/probability%20Calculation.png?raw=true"/>
</p>

Where:
* N1 is the number of times w3 occurs.
* N2 is the number of times sequence (w2,w3) occurs.
* N3 is the number of times sequence (w1,w2,w3) occurs.
* C0 is the total number of word instances in the corpus.
* C1 is the number of times w2 occurs.
* C2 is the number of times sequence (w1,w2) occurs.

All the variables are taken from the values in the context.
N1 - from the map that we initialized in the setup

N2 - if the value is pair , we take the occurence

N3 - if the value is the occurence it takes it

C0 - calculate in the setup , if it * take the occurence

C1 - from the map that we initialized in the setup

C2 - take the occurence from the value if its the correct w1 

then , the key is the 3 words and the value is the probability

## Step6
Step6 takes as input the output of Step5.
We compare with the CompareClass two strings , if the first two words are the same , we return wich one has the higher probability.
In the mapper , the output is the same key and value sorted by decending value of the probability.
