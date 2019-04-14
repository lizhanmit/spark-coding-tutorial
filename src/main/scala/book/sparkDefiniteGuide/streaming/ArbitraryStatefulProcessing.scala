package book.sparkDefiniteGuide.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

object ArbitraryStatefulProcessing {

  case class InputRow(user: String, timestamp: java.sql.Timestamp, activity: String)
  case class UserState(user: String, var activity: String, var start: java.sql.Timestamp, var end: java.sql.Timestamp)

  case class InputRow2(device: String, timestamp: java.sql.Timestamp, x: Double)
  case class DeviceState(device: String, var values: Array[Double], var count: Int)
  case class OutputRow2(device: String, previousAvg: Double)  // average of x

  case class InputRow3(uid: String, timestamp: java.sql.Timestamp, x: Double, activity: String)
  case class UserSession(val uid: String, var timestamp: java.sql.Timestamp, var values: Array[Double], var avtivities: Array[String])
  case class UserSessionOutput(val uid: String, var xAvg: Double, var activities: Array[String])

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ArbitraryStatefulProcessing")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.conf.set("spark.sql.shuffle.partitions", 5)

    import spark.implicits._

    val file = "src/main/resources/sparkDefiniteGuide/inputData/activity-data"
    val staticDF = spark.read.format("json").load(file)
    val streamingDF = spark.readStream
      .format("json")
      .schema(staticDF.schema)
      .option("maxFilesPerTrigger", 2)
      .load(file)

    val dfWithEventTime = streamingDF.selectExpr("User as user", "cast(cast(Creation_Time as double) / 1000000000 as timestamp) as timestamp", "gt as activity")


    /*
     * mapGroupsWithState
     */
    println("=== mapGroupsWithState ===")
    /*
     * find the first and last timestamp that a given user performed one of the activities in the dataset
     */
    def updateUserStateWithEvent(state: UserState, input: InputRow): UserState = {
      if (Option(input.timestamp).isEmpty) {
        return state
      }

      if (state.activity == input.activity) {
        if (input.timestamp.after(state.end)) {
          state.end = input.timestamp
        }
        if (input.timestamp.before(state.start)) {
          state.start = input.timestamp
        }
      } else {
        if (input.timestamp.after(state.end)) {
          state.start = input.timestamp
          state.end = input.timestamp
          state.activity = input.activity
        }
      }
      state
    }

    // defines the way state is updated based on an epoch of rows
    def updateAcrossEvents(user: String, inputs: Iterator[InputRow], oldState: GroupState[UserState]): UserState = {
      // here, we simply specify an old date that we can compare against
      // state here is like temp state
      var state: UserState = if (oldState.exists) oldState.get else UserState(user, "", new java.sql.Timestamp(6284160000000L), new java.sql.Timestamp(6284160L))
      for (input <- inputs) {
        state = updateUserStateWithEvent(state, input)
        oldState.update(state)
      }
      state
    }

    val mapGroupsWithStateQuery = dfWithEventTime.as[InputRow]
      .groupByKey(_.user)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateAcrossEvents)
      .writeStream
      .queryName("events_per_window")
      .format("memory")
      .outputMode("update")
      .start()

    for (i <- 1 to 5) {
      spark.sql("select * from events_per_window").show(false)
      Thread.sleep(5000)
    }


    /*
     * count-based windows
     */
    println("=== count-based windows ===")
    def updateDeviceStateWithEvent(deviceState: DeviceState, input: InputRow2): DeviceState = {
      deviceState.count += 1
      deviceState.values ++= Array(input.x)
      deviceState
    }

    def updateAcrossEvents2(device: String, inputs: Iterator[InputRow2], oldState: GroupState[DeviceState]): Iterator[OutputRow2] = {
      inputs.toSeq.sortBy(_.timestamp.getTime).toIterator.flatMap(
        input => {
          var state = if (oldState.exists) oldState.get else DeviceState(device, Array(), 0)
          state = updateDeviceStateWithEvent(state, input)
          if (state.count >= 500) {
            // clear the oldState
            oldState.update(DeviceState(device, Array(), 0))
            Iterator(OutputRow2(device, state.values.sum / state.values.length.toDouble))
          } else {
            oldState.update(state)
            Iterator()
          }
        }
      )
    }

    val mapGroupsWithStateQuery2 = streamingDF.selectExpr("Device as device", "cast(cast(Creation_Time as double) / 1000000000 as timestamp) as timestamp", "x")
        .as[InputRow2]
        .groupByKey(_.device)
        .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.NoTimeout())(updateAcrossEvents2)
        .writeStream
        .queryName("count_based_device")
        .format("memory")
        .outputMode("append")
        .start()

    for (i <- 1 to 5) {
      spark.sql("select * from count_based_device").show(false)
      Thread.sleep(5000)
    }



    /*
     * flatMapGroupsWithState
     */
    println("=== flatMapGroupsWithState, sessionzation ===")
    def updateUserSessionStateWithEvent(userSessionState: UserSession, input: InputRow3): UserSession = {
      // handle malformed dates
      if (Option(input.timestamp).isEmpty) {
        return userSessionState
      }

      userSessionState.timestamp = input.timestamp
      userSessionState.values ++= Array(input.x)
      if (!userSessionState.avtivities.contains(input.activity)) {
        userSessionState.avtivities ++= Array(input.activity)
      }
      userSessionState
    }

    def updateAcrossEvents3(uid: String, inputs: Iterator[InputRow3], oldState: GroupState[UserSession]): Iterator[UserSessionOutput] = {
      inputs.toSeq.sortBy(_.timestamp.getTime).toIterator.flatMap(
        input => {
          val state = if (oldState.exists) oldState.get else UserSession(uid, new java.sql.Timestamp(6284160000000L), Array(), Array())
          val newState = updateUserSessionStateWithEvent(state, input)

          if (oldState.hasTimedOut || state.values.length > 1000) {
            val state = oldState.get
            oldState.remove()
            Iterator(UserSessionOutput(uid, newState.values.sum / newState.values.length.toDouble, state.avtivities))
          } else {
            oldState.update(newState)
            oldState.setTimeoutTimestamp(newState.timestamp.getTime, "5 seconds")
            Iterator()
          }
        }
      )
    }

    val mapGroupsWithStateQuery3 = streamingDF.selectExpr("User as uid", "cast(cast(Creation_Time as double) / 1000000000 as timestamp) as timestamp", "x", "gt as activity")
      .as[InputRow3]
      .withWatermark("timestamp", "5 seconds")
      .groupByKey(_.uid)
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.EventTimeTimeout())(updateAcrossEvents3)
      .writeStream
      .queryName("sessionzation")
      .format("memory")
      .outputMode("append")
      .start()

    for (i <- 1 to 5) {
      spark.sql("select * from sessionzation").show(false)
      Thread.sleep(5000)
    }


    mapGroupsWithStateQuery.awaitTermination()
    mapGroupsWithStateQuery2.awaitTermination()
    mapGroupsWithStateQuery3.awaitTermination()
  }
}
