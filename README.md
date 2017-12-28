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

### step1
Step1 takes as input the 1-gram corpus and parses it line by line.
For each line from the 1-gram corpus , it creates a line with the word and its occurrence , and another line with  * and the same occurrence , and sends it to the reducer.
In the reducer , we combine all the occurences by key. In addition , we sum up all the occurrences with * in order to get the sum of all the words occurences.

### step2
