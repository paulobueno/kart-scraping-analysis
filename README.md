# Likelyhood of winning a championship based on car's performance history


###The main objective of this analysis is to measure how much impact the deviation on car's performance determine championship's results.

The approach consists on identify how much in seconds each car is faster based on all races and then select one championship to predict what would be the result if there were no performance diferences between cars.

The data was obtained scraping [KGV's website](http://kartodromogranjaviana.com.br/), one of the biggest kart circuits in Latin America, based on Granja Viana - Sao Paulo.

At KGV kart circuit there are several car categories that are allowed to performe a race. To that analysis, we are going to focus only on races in which rented cars were used. The reason is that by following this approach we have a better chance to correctly identify and link cars between diferent races. Furthermore, the data of rented races is much richier than other categories.

To tackle our objective, we are going to base our analysis on KGV's Granja Viana circuit in 2021, restricted to rented car's races.

###The analysis will be composed by two main parts:
1. Likelihood of a car win
2. Likelihood of a racer win 
3. Likelihood of a racer win a championship, based on his race's cars history on a given period of time

###We are going to use the following assumptions:
* Car's numbers doesn't change between the actual cars (confirmed by local adms)
* Championship points scoring system is equal to [F1 in 2019](https://en.wikipedia.org/wiki/List_of_Formula_One_World_Championship_points_scoring_systems)  

###Possible biases to check:
* Some cars race more often than others
* Some cars have a shorter life cycle than others
* Some cars are repaired earlier than others

## Downloading data
```shell
python main.py
Configs loaded:
 circuit: granjaviana
 domain: http://www.kartodromogranjaviana.com.br/resultados
 init: 2021-01-01
 end: 2021-12-31
-------------------- Collecting UIDs --------------------
100%|██████████| 245/245 [04:04<00:00,  1.00it/s]
-------------------- Collecting Results --------------------
100%|██████████| 3776/3776 [1:03:03<00:00,  1.00s/it]
Saved to CSV on: ./Data/data.csv

Process finished with exit code 0
```


