package com.ikas.ai.spark

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{MinMaxScaler, StandardScaler, VectorAssembler}
import org.apache.spark.ml.param.ParamPair
import org.apache.spark.ml.regression.{RandomForestRegressionModel => RFRModel, RandomForestRegressor => RFR}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

class RandomForestRegressor(table_name: DataFrame,
                            hyper_params_config: Option[Map[String, Any]] = None,
                            model_path_dir: String = "",
                            model_filename_prefix: String = "",
                            tree_nums: Int = 10,
                            max_depth: Int = 25,
                            scaler_type: Int = 0,
                            feature_subset_strategy: String = "all",
                            seed: Int = 1234) extends SparkBaseOperator {

  // 检查是否有超参数传递
  if (hyper_params_config.isEmpty) {
    // 没有超参数传递, 正常训练， 入参审查
    assert(table_name.isInstanceOf[DataFrame], "数据应为Spark数据框且不为空，请检查")
    assert(model_path_dir != "" && model_path_dir.isInstanceOf[String], "请输入正确的目录")
    assert(model_filename_prefix != "" && model_filename_prefix.isInstanceOf[String], "请输入正确的模型文件名")
    assert(tree_nums.isInstanceOf[Int] && (1 <= tree_nums && tree_nums <= 1000), "树的棵树应为1—1000之间的整数，请检查")
    assert(max_depth.isInstanceOf[Int] && (1 <= max_depth && max_depth <= 30), "树的最大深度应为1—30之间的整数，请检查")
    assert(scaler_type == 0 || scaler_type == 1 || scaler_type == 2, "标准化方法只能是 0,1,2 其中之一")
    assert(feature_subset_strategy == "all" || feature_subset_strategy == "sqrt" || feature_subset_strategy == "log2", "特征选择策略必须在 ['all', 'sqrt', 'log2']之间")
    assert(seed.isInstanceOf[Int] && seed > 0)
  }

  def get_model_best_params(best_model:RFRModel, hyper_params:Map[String, Any]) : DataFrame = {
    val ss:SparkSession = getSpark()

    val paramMap = best_model.extractParamMap()
//    val bestParamsMap = mutable.Map[String, Any]()
    // 打印模型的超参数
    val param_row = ss.sparkContext.parallelize(paramMap.toSeq.map { case ParamPair(param, value) =>
//      只筛选出用户选择的超参
     Row(param.name, value.toString)
    })

//    println(paramMap.toSeq.filter(_ => hyper_params.contains(_.param.name)))

    val schema = StructType(Seq(
      StructField("best_params", StringType, nullable = false),
      StructField("value", StringType, nullable = false)
    ))

    val bestParams:DataFrame = ss.createDataFrame(param_row, schema)
    //            println(paramMap)

     bestParams
  }
  //
  def get_model_feature_importance(forest_model: RFRModel, input_cols: Array[String]): DataFrame  = {

//    获取spark session
    val ss:SparkSession = getSpark()

    //    spark_session.createDataFrame()
    val schema = StructType(Seq(
      StructField("features", StringType, nullable = true),
      StructField("importance", DoubleType, nullable = true)
    ))

    val data = input_cols.zip(forest_model.featureImportances.toArray)
//    println("feature importance", forest_model.featureImportances.toDense.toString())

    val rdd = ss.sparkContext.parallelize(data.map { row => Row.fromTuple(row)})

    val feature_importance:DataFrame = ss.createDataFrame(rdd, schema).orderBy(desc("importance"))



    feature_importance

  }

  def run(): Unit = {

    // 从元数据信息获取自变量和因变量
    val (input_cols, label_col, _) = com.ikas.ai.utils.metadataRole.getColRoleListFromRoleMetadata(table_name)
    assert(input_cols.length > 0, "No input variables")
    assert(label_col.length == 1, "No output variable or multiple output variables")
    val labelCol = label_col(0)

    if (hyper_params_config.isEmpty) {
      // 变量映射
      val assembler = new VectorAssembler().setInputCols(input_cols.toArray).setOutputCol("features")
      // 根据不同预处理方式构造模型训练管道
      val pipe = if (scaler_type == 0) { // 不做任何预处理

        val rf = new RFR()
          .setFeaturesCol("features")
          .setLabelCol(labelCol)
          .setNumTrees(tree_nums)
          .setMaxDepth(max_depth)
          .setSeed(seed)
          .setFeatureSubsetStrategy(feature_subset_strategy)

        new Pipeline().setStages(Array(assembler, rf))
      } else { // 附加预处理

        val scaler = if (scaler_type == 1) {
          // 归一化预处理
          new MinMaxScaler().setInputCol("features").setOutputCol("scaledOutput")
        } else { // 标准化归一化
          new StandardScaler().setInputCol("features").setOutputCol("scaledOutput")
        }

        val rf = new RFR()
          .setFeaturesCol("scaledOutput")
          .setLabelCol(labelCol)
          .setNumTrees(tree_nums)
          .setMaxDepth(max_depth)
          .setSeed(seed)
          .setFeatureSubsetStrategy(feature_subset_strategy)

        new Pipeline().setStages(Array(assembler, scaler, rf))
      }
      // 训练模型
      val model = pipe.fit(table_name)
      val save_path = model_path_dir + "/" + model_filename_prefix

      // 特征重要性提取
      val feature_importance = get_model_feature_importance(model.stages.last.asInstanceOf[RFRModel], input_cols.toArray)
//      val feature_importance = None

      // 模型保存
      try {
        model.write.overwrite.save(save_path)
      } catch {
        case e: Exception => throw new IllegalArgumentException("模型保存失败：\n" + e.getMessage)
      }
      outputs = mutable.Map(
        "model_path" -> save_path,
        "table_name" -> feature_importance,
        "best_params_table_name" -> None // 为空不输出
      )

  } else {


      val save_path = model_path_dir + "/" + model_filename_prefix
      val hyper_params: Map[String, List[Any]] = hyper_params_config.get("hyper_params").asInstanceOf[Map[String, List[AnyVal]]]
      if (hyper_params.isEmpty) {
        throw new IllegalArgumentException("hyper_params in hyper_params_config must not be empty")
      }
      // 调参方法
      val optimize_method = hyper_params_config.get("optimize_method").asInstanceOf[String]
      // 折数
      val kfold = hyper_params_config.get("kfold").asInstanceOf[Int]
      // 划分比列
      val random_split_ratio = hyper_params_config.get("random_split_ratio").asInstanceOf[Double]
      // 并行度
      val n_jobs = hyper_params_config.get("n_jobs").asInstanceOf[Int]
      // 随机数种子
      val seed = hyper_params_config.get("seed").asInstanceOf[Int]
      // 评估指标
      // val eval_metric = table_name_P("eval_metric").values(0)
      if (random_split_ratio + kfold == 0) {
        throw new IllegalArgumentException("交叉验证模式和留出法验证模型二选一，不能全不选")
      }

      // 检查范围，包含未选值
      assert(0 <= random_split_ratio && random_split_ratio < 1, "留出法比例必须为[0, 1)之间")
      assert(1 < kfold || kfold == 0, "交叉验证模式折数必须大于1")

      // 检测验证模式
      val flag = if (random_split_ratio > 0.0 && kfold == 0) {
        0 // 留出法模式
      } else if (random_split_ratio == 0.0 && kfold > 0) {
        1 // 交叉验证模式
      } else {
        throw new IllegalArgumentException("留出法模式和交叉验证模式只能二选一，不能全选")
      }

      // 变量映射
      val assembler = new VectorAssembler().setInputCols(input_cols.toArray).setOutputCol("features")
      val table_name_renamed = table_name.withColumnRenamed(labelCol, "label")
      // todo: 后续模型加载会遇到这类问题
      // 交叉验证模式下不做预处理 # 预测列，验证模式下，只能将预测列专为'label'列
      val rf = new RFR().setFeaturesCol("features").setLabelCol("label").setSeed(seed)
      // 构建pipeline
      val pipe = new Pipeline().setStages(Array(assembler, rf))

      if (optimize_method == "grid") {
        // 构建网格

        val paramGrid = hyper_params.foldLeft(new ParamGridBuilder()) {
          case (builder, (param, v)) => param match {
            case "maxDepth" => builder.addGrid(rf.getParam(param), v.asInstanceOf[List[Int]])
            case "numTrees" => builder.addGrid(rf.getParam(param), v.asInstanceOf[List[Int]])
            case "featureSubsetStrategy" => builder.addGrid(rf.getParam(param), v.asInstanceOf[List[String]])
          }

        }.build()


        // 网格方法
        flag match {
          case 0 => {
            val val_model = new TrainValidationSplit()
              .setEstimator(pipe)
              .setEstimatorParamMaps(paramGrid)
              .setTrainRatio(random_split_ratio)
              .setEvaluator(new RegressionEvaluator())
              .setParallelism(n_jobs)

            val best_model = val_model.fit(table_name_renamed).bestModel.asInstanceOf[PipelineModel]
            val feature_importance = get_model_feature_importance(best_model.stages.last.asInstanceOf[RFRModel], input_cols.toArray)

//            val a = hyper_params.keys.map { k =>
//                        k -> best_model.getDefault(best_model.getParam(k))
//                      }.toSeq
           val bestParams = get_model_best_params(best_model.stages.last.asInstanceOf[RFRModel]:RFRModel, hyper_params)
            println(bestParams)


            outputs = mutable.Map(
              "model_path" -> save_path,
              "table_name" -> feature_importance,
              "best_params_table_name" -> bestParams // 选择的最优参数
            )

          }
          case 1 => {
            val val_model = new CrossValidator()
              .setEstimator(pipe)
              .setEstimatorParamMaps(paramGrid)
              .setEvaluator(new RegressionEvaluator())
              .setNumFolds(kfold)
              .setParallelism(n_jobs)

            val best_model = val_model.fit(table_name_renamed).bestModel.asInstanceOf[PipelineModel]

            val feature_importance = get_model_feature_importance(best_model.stages.last.asInstanceOf[RFRModel], input_cols.toArray)

            val bestParams:DataFrame = get_model_best_params(best_model.stages.last.asInstanceOf[RFRModel]:RFRModel, hyper_params)


            outputs = mutable.Map(
              "model_path" -> save_path,
              "table_name" -> feature_importance,
              "best_params_table_name" -> bestParams // 选择的最优参数
            )

          }

        }


      }

    }
  }

}

object RandomForestRegressor {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("随机森林调整")
      .config("spark.sql.session.timeZone", "Asia/Shanghai")
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.driver.memory", "1024m")
      .config("spark.driver.cores", "4")
      .config("spark.executor.memory", "1024m")
      .config("spark.executor.cores", "1")
      .config("spark.cores.max", "2")
      .config("spark.driver.host", "192.168.28.49")
      .master("spark://192.168.11.200:7077,192.168.11.201:7077")
      .appName("local[4]")
      .getOrCreate()


    val startTime = System.currentTimeMillis()

    val data = Seq(
      Row(1.0f, 2.0f, 3.0f, 4.0f, 5.0f),
      Row(2.0f, 3.0f, 4.0f, 5.0f, 6.0f),
      Row(3.0f, 4.0f, 5.0f, 6.0f, 7.0f)
    )

    val schema = StructType(Seq(
      StructField("x1", FloatType, nullable = false),
      StructField("x2", FloatType, nullable = false),
      StructField("x3", FloatType, nullable = false),
      StructField("x4", FloatType, nullable = false),
      StructField("y", FloatType, nullable = false)
    ))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    val hyperParamsConfig: Map[String, Any] = Map(
      "hyper_params" -> Map("maxDepth" -> List(5, 10, 15),
        "numTrees" -> List(10, 30, 100),
        "featureSubsetStrategy" -> List("sqrt", "log2", "all")),
      "eval_metric" -> "", "optimize_method" -> "grid",
      "n_jobs" -> 2, "seed" -> 2023, "kfold" -> 0,
      "random_split_ratio" -> 0.8)
    val new_df = com.ikas.ai.utils.metadataRole.insertRoleMetaData(df, inputCols = List("x1", "x2", "x3", "x4"), outputCols = List("y"))

    //    val hyperParamsConfig: Option[Map[String, Any]] = None

    val params = Map("table_name" -> new_df, "model_path_dir" -> "hdfs://192.168.11.200:8020/user/xian.huafeng",
      "model_filename_prefix" -> "rfr_model3", "tree_nums" -> 10, "max_depth" -> 3,
      "scaler_type" -> 1, "hyper_params_config" -> hyperParamsConfig, "feature_subset_strategy" -> "all",
      "seed" -> 1234
    )


    val rfr = new RandomForestRegressor(
      table_name = params("table_name").asInstanceOf[DataFrame], hyper_params_config = Some(hyperParamsConfig), model_path_dir = params("model_path_dir").asInstanceOf[String], model_filename_prefix = params("model_filename_prefix").asInstanceOf[String],
      tree_nums = params("tree_nums").asInstanceOf[Int], max_depth = params("max_depth").asInstanceOf[Int], scaler_type = params("scaler_type").asInstanceOf[Int],
      feature_subset_strategy = params("feature_subset_strategy").asInstanceOf[String], seed = params("seed").asInstanceOf[Int]
    )
    rfr.setSpark(spark)
    rfr.run()

    val endTime = System.currentTimeMillis()

    println((endTime - startTime)/ 1000)
//    println(spark)
//
//    println("------------------------------------here------------------------------------------")
//    println(df.getClass.getName, df.show())
    val result:Map[String, Any] = rfr.getOutputs().toMap

    for ((k, v) <- result){
        v match {
          case new_k: DataFrame =>
            new_k.show()
          case m:String => println(m)
          case _ =>
        }
    }

  }



}