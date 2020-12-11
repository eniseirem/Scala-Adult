val adultDF = spark.read.option("header",false).csv("adult (1).data")
//there is no header on the original dataset, however the headers are given as a info. I'll add the header to more understable actions.
val AdultDF = adultDF.toDF("age","workclass",
"-1",
"education",
"education-num",
"marital-status",
"occupation",
"relationship",
"race",
"sex",
"-5",
"-3",
"-4",
"native-country",
"income")

//I'll drop the unnecessary parts.
val filterdDF = AdultDF.drop("age","workclass","-1","marital-status","occupation","-5","-3","-4","native-country")
//if any na value;
val df0 = filterdDF.na.drop
val df_1 = df0.withColumn("race", when($"race" === " White", "White").otherwise("Non-White"))


df_1.groupBy("sex").count.show
//+-------+-----+
//|    sex|count|
//+-------+-----+
//|   Male|21790|
//| Female|10771|
//+-------+-----+

//they are not equal so lets divide them into two to see gender related effect first

val df_male =. df_1.filter(df_1("sex")===" Male") 
val df_female = df_1.filter(df_1("sex")===" Female") 
 
val df_male_analysis = df_male.groupBy("income").count()

df_male_analysis.groupBy("income").agg(sum("count").alias("count")).withColumn("percentage"
, col("count") / sum("count").over()).show

//+------+-----+------------------+
//|income|count|        percentage|
//+------+-----+------------------+
//| <=50K|15128|0.6942634235888022|
//|  >50K| 6662|0.3057365764111978|
//+------+-----+------------------+

//as you can see approximately 69% are less paid. Let's see the womens situation.
val df_female_analysis = df_female.groupBy("income").count()

df_female_analysis.groupBy("income").agg(sum("count").alias("count")).withColumn("percentage"
, col("count") / sum("count").over()).show

// percentage increased 20% with women. So women are most likely less paid then men.
//let's see how many of women or men are get educate.
val df_female_ed = df_female.groupBy("education").count()

df_female_ed.groupBy("education").agg(sum("count").alias("count")).withColumn("percentage"
, col("count") / sum("count").over()).show

//+-------------+-----+--------------------+
//|    education|count|          percentage|
//+-------------+-----+--------------------+
//|    Bachelors| 1619| 0.15031102033237398|
//|      Masters|  536|0.049763253179834745|
//|          9th|  144|0.013369232197567542|
//|      HS-grad| 3390| 0.31473400798440254|
//| Some-college| 2806|  0.2605143440720453|
//|  Prof-school|   92|0.008541453904001486|
//|         11th|  432| 0.04010769659270263|
//|    Doctorate|   86| 0.00798440256243617|
//|   Assoc-acdm|  421| 0.03908643579983288|
//|         10th|  295|0.027388357626961284|
//|    Assoc-voc|  500|0.046420945130442856|
//|      1st-4th|   46|0.004270726952000743|
//|    Preschool|   16|0.001485470244174...|
//|      5th-6th|   84|  0.0077987187819144|
//|      7th-8th|  160|0.014854702441741714|
//|         12th|  144|0.013369232197567542|
//+-------------+-----+--------------------+

// 15% Bachelor, almost 5% masters, 0.8% doctorate... 

val df_male_ed = df_male.groupBy("education").count()

df_male_ed.groupBy("education").agg(sum("count").alias("count")).withColumn("percentage"
, col("count") / sum("count").over()).show

//+-------------+-----+--------------------+
//|    education|count|          percentage|
//+-------------+-----+--------------------+
//|    Bachelors| 3736| 0.17145479577787975|
//|      HS-grad| 7111|  0.3263423588802203|
//|         11th|  743| 0.03409821018815971|
//| Some-college| 4485| 0.20582836163377696|
//|   Assoc-acdm|  646|0.029646626893070217|
//|    Assoc-voc|  882|0.040477283157411656|
//|      7th-8th|  486|0.022303809086737035|
//|    Doctorate|  327|0.015006883891693438|
//|          9th|  370| 0.01698026617714548|
//|      5th-6th|  249|0.011427260211106013|
//|         10th|  638|0.029279486002753558|
//|      Masters| 1187| 0.05447452960073428|
//|  Prof-school|  484| 0.02221202386415787|
//|      1st-4th|  122| 0.00559889857732905|
//|         12th|  289|0.013262964662689307|
//|    Preschool|   35|0.001606241395135...|
//+-------------+-----+--------------------+

// 17% Bachelor, 0.5% Master which is way more lower than womens. Also with doctorate 0.01% much more lower then women. 

//So this means that even a woman more qualified then man, she'll probably get lower salary.

// 20% percent differance between man and women is huge. Let's check women data more to understand who is the lowest. 

val df_female_fam = df_female.groupBy("relationship","income").count

df_female_fam.groupBy("relationship","income").agg(sum("count").alias("count")).withColumn("percentage"
, col("count") / sum("count").over()).show

//+---------------+------+-----+--------------------+
//|   relationship|income|count|          percentage|
//+---------------+------+-----+--------------------+
//|           Wife| <=50K|  822| 0.07631603379444805|
//|  Not-in-family| <=50K| 3591| 0.33339522792684056|
//|  Not-in-family|  >50K|  284|0.026367096834091542|
//|      Own-child| <=50K| 2220| 0.20610899637916627|
//|      Unmarried|  >50K|  112|  0.0103982917092192|
//|      Unmarried| <=50K| 2542| 0.23600408504317147|
//|           Wife|  >50K|  744| 0.06907436635409897|
//| Other-relative| <=50K|  416| 0.03862222634852846|
//|      Own-child|  >50K|   25|0.002321047256522...|
//| Other-relative|  >50K|   14|  0.0012997864636524|
//|        Husband| <=50K|    1|9.284189026088571E-5|
//+---------------+------+-----+--------------------+

//lowest change of getting a higher salary is when you have child. You should be "not-in-family" to get higher chance.


val df_male_fam = df_male.groupBy("relationship","income").count

df_male_fam.groupBy("relationship","income").agg(sum("count").alias("count")).withColumn("percentage"
, col("count") / sum("count").over()).show

//+---------------+------+-----+--------------------+
//|   relationship|income|count|          percentage|
//+---------------+------+-----+--------------------+
//|  Not-in-family| <=50K| 3858| 0.17705369435520882|
//|        Husband| <=50K| 7274|  0.3338228545204222|
//|        Husband|  >50K| 5918|  0.2715924736117485|
//|      Own-child| <=50K| 2781|  0.1276273519963286|
//|      Unmarried| <=50K|  686| 0.03148233134465351|
//|  Not-in-family|  >50K|  572| 0.02625057365764112|
//| Other-relative| <=50K|  528|0.024231298760899495|
//| Other-relative|  >50K|   23|0.001055530059660...|
//|      Unmarried|  >50K|  106|0.004864616796695732|
//|           Wife|  >50K|    1|4.589261128958237...|
//|      Own-child|  >50K|   42| 0.00192748967416246|
//|           Wife| <=50K|    1|4.589261128958237...|
//+---------------+------+-----+--------------------+

//same with men. But if (I'm ignoring the wife section since the count is only 1) you're married and you're a husband then you have 33% to get higher salary.

//we check datas to make comments. lets deep dive and see the this in a more scientific way

val df_2 = df_1.withColumn("income", when($"income" === " <=50K", 0).otherwise(1))
val df_3 = df_2.withColumn("sex", when($"sex" === " Female", 0).otherwise(1))
val df_4 = df_3.withColumn("sex", when($"race" === "Non-White", 0).otherwise(1))

//only to keep integer to evaluate better.
val df_mod = df_4.drop("education","relationship","education-num")



//LOGISTIC REGRESSION (Accuracy is 50% so not a good model)------------
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LogisticRegression

//defining our features columns
val cols = Array("race", "sex")
val assembler = new VectorAssembler()
  .setInputCols(cols)
  .setOutputCol("features")
val featureDf = assembler.transform(df_mod)
featureDf.printSchema()


val indexer = new StringIndexer()
  .setInputCol("income")
  .setOutputCol("label")
val labelDf = indexer.fit(featureDf).transform(featureDf)
labelDf.printSchema()


//lets find our accuracy 
val evaluator = new BinaryClassificationEvaluator()
  .setLabelCol("label")
  .setRawPredictionCol("prediction")
  .setMetricName("areaUnderROC")
  

// I want to see the training datas accuracy first but it is too low for train data. So this model is not good enough to test with it.
val logisticRegressionModel = logisticRegression.fit(labelDf)


val predictionDf = logisticRegressionModel.transform(tdf)
predictionDf.show(10)
val accuracy = evaluator.evaluate(predictionDf)
println(accuracy)

//50% percent is not good enough. So this model should be improved

//But from our earlier findings we can still say that, according to data; women are has less chance to get higher salaries. 
// The main difference between man and woman is not the education (which higher for women) or race, even though non-white people 
// clearly less paid. The main gap is the family. If a women has family, a child, her salary is lower than 50k but if a man has family
// the payment increases. So effect is opposite for each gender. 

