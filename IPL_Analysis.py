# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# COMMAND ----------

spark=SparkSession.builder.appName('IPL_DataAnalysis').getOrCreate()

# COMMAND ----------

spark

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.sparkdataanalysis.dfs.core.windows.net",
    "cNrcpNDjpHGOdcnzFEITQh+BzNwjV1sepCn350RndqSSqzdyD1/xbT2f7Myjr59PIO5j/cbrRqyZ+AStfT6iGQ==")


# COMMAND ----------

ball_by_ball=spark.read.csv("abfss://ipldata@sparkdataanalysis.dfs.core.windows.net/Ball_By_Ball.csv",header=True,inferSchema=True)

# COMMAND ----------

ball_by_ball.display()

# COMMAND ----------

match_df = spark.read.format("csv").option("inferSchema","true").option("header","true").load("abfss://ipldata@sparkdataanalysis.dfs.core.windows.net/Match.csv")

     

# COMMAND ----------

match_df.display()

# COMMAND ----------

player_match_df=spark.read.format('csv').option('inferschema','true').option('header','true').load("abfss://ipldata@sparkdataanalysis.dfs.core.windows.net/Player_match.csv")

# COMMAND ----------

player_match_df.display()

# COMMAND ----------

player_df=spark.read.format('csv').option('header','true').option('inferschema','true').load("abfss://ipldata@sparkdataanalysis.dfs.core.windows.net/Player.csv")

# COMMAND ----------

player_df.display()

# COMMAND ----------

team_df=spark.read.csv("abfss://ipldata@sparkdataanalysis.dfs.core.windows.net/Team.csv",header=True,inferSchema=True)

# COMMAND ----------

team_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Filter to include only valid deliveries (excluding extras like wides and no balls for specific analyses)

# COMMAND ----------

from pyspark.sql.functions import col, when, sum, avg, row_number 
ball_by_ball=ball_by_ball.filter((col("wides") == 0) & (col("noballs")==0))

# COMMAND ----------

ball_by_ball.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Calculate the total and average runs scored in each match and inning

# COMMAND ----------

total_avg_runs=ball_by_ball.groupBy('match_id','innings_no').agg(sum('Runs_Scored').alias('total_runs'), avg('Runs_Scored').alias('avg_runs')).orderBy('total_runs',ascending=[False])
  

# COMMAND ----------

total_avg_runs.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Calculate running total of runs in each match for each over

# COMMAND ----------

running_total=ball_by_ball.groupby('match_id','over_id').agg(sum('Runs_Scored').alias('total_runs')).orderBy('total_runs',ascending=False)

# COMMAND ----------

running_total.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Either a wicket or more than 6 runs including extras

# COMMAND ----------

impact=ball_by_ball.withColumn('impact',  when((col("runs_scored") + col("extra_runs") > 6) | (col("bowler_wicket") == True), True).otherwise(False))
                    

# COMMAND ----------

impact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####categorizing win margins into 'high', 'medium', and 'low'

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth
win_margins=match_df.withColumn('win_margins', when(col('Win_Margin')>=100, "high").when((col('Win_Margin')>= 50) & (col('Win_Margin')<100), "medium").otherwise('low'))

# COMMAND ----------

win_margins.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Analyze the impact of the toss: who wins the toss and the match

# COMMAND ----------

toss_match_wins=match_df.withColumn('toss_match_wins', when(col('Toss_Winner')==col('match_winner'),'Yes').otherwise('No'))

# COMMAND ----------

toss_match_wins.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####count of left handed and right handed batsmen
# MAGIC

# COMMAND ----------

batting=player_df.groupby('batting_hand').count()

# COMMAND ----------

batting.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Handling missing values in Batting hand and bowling skill
# MAGIC

# COMMAND ----------

batting_fill=player_df.na.fill({'batting_hand' : 'unknown','bowling_skill' : 'unknown'})

# COMMAND ----------

batting_fill.where(batting_fill['bowling_skill'] == 'unknown').count()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Add a 'veteran_status' column based on player age

# COMMAND ----------

player_match_df.withColumn('Veteran_status', when(col('age_as_on_match') >= 35, 'Veteran').otherwise('Non-Veteran')).display()

# COMMAND ----------

player_match_df=player_match_df.na.fill({'batting_status':'unknown','bowling_status':'unknown'})

# COMMAND ----------

player_match_df.display()
