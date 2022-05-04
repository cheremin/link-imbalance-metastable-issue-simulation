package ru.cheremin.mrupool


import krangl.*
import org.kalasim.*
import org.kalasim.analysis.ResourceActivityEvent
import java.util.concurrent.TimeUnit.MILLISECONDS
import kotlin.math.pow
import kotlin.math.roundToInt
import kotlin.math.sqrt
import kotlin.random.Random

class Query(val pool: Pool, val dbTime: Number, val networkTime: Number) : Component() {
    //  TODO   val _pool: MRUPool by inject()

    private var fiberDelay: Number = 0

    override fun process() = sequence {
        request(pool) {
            //connection is guaranteed to be available inside request(pool)
            val connection = pool.take()
            val fiber = connection.goingThroughFiber
            try {
                //Client -> network request -> DB
                request(fiber) {
                    hold(networkTime.toDouble() + fiber.delay.toDouble(), "going to DB")
                }

                hold(dbTime, "process query in DB") //assume DB is ideally parallelized

                //DB -> network response -> client
                request(fiber) {
                    hold(networkTime.toDouble() + fiber.delay.toDouble(), "going back from DB")
                }
                fiberDelay = fiber.delay
            } finally {
                pool.release(connection)
            }
        }
    }

    fun soloExecutionTime(): Double =
        (2 * networkTime.toDouble() + dbTime.toDouble() + fiberDelay.toDouble())
}

open abstract class Pool : Resource {
    constructor(name: String, capacity: Int) : super(name, capacity)

    abstract fun take(): Connection
    abstract fun release(conn: Connection)

    abstract val connections: Iterable<Connection>
}


class MRUPool : Pool {
    private val _size: Int
    override val connections: ArrayDeque<Connection> = ArrayDeque()

    constructor(connections: List<Connection>)
            : super("MRUPool", capacity = connections.size) {
        _size = connections.size
        this.connections.addAll(connections)
    }

    constructor(connectionFactory: () -> Connection, poolSize: Int)
            : this(generateSequence { connectionFactory() }.take(poolSize).toList())

    override fun take(): Connection {
        if (connections.isEmpty()) {
            throw IllegalStateException("Pool is empty: .take() without .request()?")
        }
        return connections.removeFirst()
    }

    override fun release(conn: Connection) {
        connections.addFirst(conn)
    }
}


class MRUPoolAutoSizing : Pool {
    private val connectionFactory: () -> Connection

    private val minSize: Int
    private val maxSize: Int
    private val idleConnectionTimeout: Double

    private val connectionsStack: ArrayDeque<ConnectionHolder> = ArrayDeque()

    override val connections
        get() = connectionsStack.map { it.connection }


    data class ConnectionHolder(
        val connection: Connection,
        val lastUsedTick: TickTime
    ) : Comparable<ConnectionHolder> {
        override fun compareTo(other: ConnectionHolder) = lastUsedTick.compareTo(other.lastUsedTick)
    }

    constructor(
        connectionFactory: () -> Connection,
        idleConnectionTimeout: Double,
        maxSize: Int = Int.MAX_VALUE,
        minSize: Int = 0
    ) : super("MRUPoolAutoSizing", capacity = maxSize) {
        require(minSize >= 1) { "minSize(=$minSize) must be >=1" }
        require(maxSize >= minSize) { "maxSize(=$maxSize) must be >=minSize(=$minSize)" }
        require(idleConnectionTimeout >= 1) { "idleConnectionTimeout(=$idleConnectionTimeout) must be >=1" }

        this.connectionFactory = connectionFactory
        this.maxSize = maxSize
        this.minSize = minSize
        this.idleConnectionTimeout = idleConnectionTimeout

        for (i in 1..minSize) {
            connectionsStack.add(ConnectionHolder(connectionFactory(), now));
        }
    }

    override fun take(): Connection {
        if (connectionsStack.isEmpty()) {
            connectionsStack.add(
                ConnectionHolder(
                    connectionFactory(),
                    now
                )
            )
        }
        return connectionsStack.removeFirst().connection
    }

    override fun release(conn: Connection) {
        connectionsStack.addFirst(ConnectionHolder(conn, now))

        //drop too old connections, if any:
        while (connectionsStack.size > minSize) {
            val lruEntry = connectionsStack.last()
            if (now - lruEntry.lastUsedTick > idleConnectionTimeout) {
                connectionsStack.removeLast()
            } else {
                break;
            }
        }
    }

//    override fun toString(): String {
//        return "${name}: ${_size} connections, ${_size-connections.size} in use: ${connections}"
//    }
}

class Connection(val name: String, val goingThroughFiber: Fiber) {
    override fun toString() = name
}

class Fiber(val _name: String, val delay: Number) : Resource(_name, capacity = 1) {
    override fun toString() = "Fiber[$_name][delay:$delay]"
}

// ========================== infra/helpers ==============================================

fun main() {
    val fibersCount = 5
    val maxPoolSize = 100

    val meanInterArrivalTime = 15
    val meanDbTime = 5
    val meanNetworkTime = 11

    val TOTAL_SIMULATION_TIME = 2_000_000.0

    val BATCHES_OF_QUERIES_COUNT = 4  // how many 'batches' of queries to issue
    val MEAN_INTER_ARRIVAL_TIME_FOR_BATCHES = 200_000
    val QUERIES_IN_BATCH = maxPoolSize

    val utilization = (meanNetworkTime * 2.0 + meanDbTime) / meanInterArrivalTime / fibersCount
    println(
        "Utilization (regular queries) = ${(utilization * 100).roundToInt()}%\n" +
                "\tmeanIAT=$meanInterArrivalTime\n" +
                "\tnetwork=$meanNetworkTime -> db=$meanDbTime -> network=$meanNetworkTime"
    )

    val randomSeed = Random.nextInt()
    val simulation = object : Environment(randomSeed = randomSeed) {
        val fibers: Array<Fiber> = Array(fibersCount) { i ->
            //break symmetry: make some fibers slightly slower than others
            Fiber("$i", delay = i * 1.0 / fibersCount)
        }

        //        val pool = MRUPool(
//            Array(poolSize) { i ->
//                val fiberIndex = i % fibers.size
//                val fiber = fibers[fiberIndex]
//                Connection("conn[$i] via $fiber", fiber)
//            }.toList()
//        )
        var connectionIndex = 0;
        val pool = MRUPoolAutoSizing(
            {
                val fiberIndex = (connectionIndex++) % fibers.size
                val fiber = fibers[fiberIndex]
                Connection("conn[$connectionIndex] via $fiber", fiber)
            },
            idleConnectionTimeout = 10_000.0,
            maxPoolSize,
            fibersCount * 2
        )

        init {
            tickTransform = TickTransform(MILLISECONDS)

            val interArrivalPD = exponential(meanInterArrivalTime)
            val interArrivalBatchPD = constant(MEAN_INTER_ARRIVAL_TIME_FOR_BATCHES)
//            val networkTimePD = triangular(meanNetworkTime - 1, meanNetworkTime, meanNetworkTime + 1)
            val networkTimePD = constant(meanNetworkTime)
//            val dbTimePD = triangular(meanDbTime - 1, meanDbTime, meanDbTime + 1)
            val dbTimePD = constant(meanDbTime)

            //'Regular' queries, one-by-one:
            ComponentGenerator(iat = interArrivalPD, until = TickTime(TOTAL_SIMULATION_TIME)) {
                Query(
                    pool,
                    dbTime = dbTimePD.sample(),
                    networkTime = networkTimePD.sample()
                )
            }

            //'Login' queries: batch of queries
            ComponentGenerator(
                iat = interArrivalBatchPD,
//                startAt = TickTime(100_000.0),
                total = BATCHES_OF_QUERIES_COUNT
            ) {
                for (i in 1..QUERIES_IN_BATCH) {
                    Query(
                        pool,
                        dbTime = dbTimePD.sample(),
                        networkTime = networkTimePD.sample()
                    )
                }
            }
        }
    }

    //== SIMULATION: =============================================================

    println(simulation.pool.connections)
    simulation.run()
    println(simulation.pool.connections)

    //== RESULTS: ================================================================

    val poolActivities = simulation.pool.activities

    //calculate real utilization:
    val totalExecutionTime = poolActivities.sumOf {
        (it.requester as Query).soloExecutionTime()
    }
    val totalSimulatedTime = poolActivities.maxOf { it.released } - poolActivities.minOf { it.requested }
    val realRho = totalExecutionTime / totalSimulatedTime / fibersCount
    println("Real utilization: ${(realRho * 100).roundToInt()}%")

    simulation.fibers.forEachIndexed { index, fiber ->
        val connectionsGoingThrough = simulation.pool.connections.count { it.goingThroughFiber == fiber }
        val totalTimeInUse = fiber.activities.sumOf { it.released - it.honored }
        val totalTimeWaited = fiber.activities.sumOf { it.honored - it.requested }

        //Interestingly, totalTimeWaited expected to be highest for most utilized
        //       fibers, but it doesn't. Probably, it is because most of waiting time
        //       is collected during spikes, but most utilization is during regular
        //       traffic?
        println(
            "Fiber[$index]: served ${fiber.activities.distinctBy { it.requester }.count()} queries, " +
                    "through $connectionsGoingThrough connections, " +
                    "utilization ${(totalTimeInUse * 100.0 / totalSimulatedTime).roundToInt()}%, " +
                    "mean waiting ${(totalTimeWaited * 100.0 / totalTimeInUse).roundToInt()}%"
        )
    }

    //Print (processing time/raw processing time) averaged per bucket:
    val df = extractStatsFromPoolActivityTimeline(poolActivities, bucketSize = 50_000)
    df.print(rowNumbers = false, maxRows = Int.MAX_VALUE)
}


fun extractStatsFromPoolActivityTimeline(
    poolActivities: List<ResourceActivityEvent>,
    bucketSize: Int
): DataFrame {
    val df = poolActivities.groupBy { it.time.value.toInt() / bucketSize }
        .map { (bucket, values) ->
            val totalWaitingTime = values.sumOf {
                (it.released - it.requested) - (it.requester as Query).soloExecutionTime()
            }
            val totalExecutionTime = values.sumOf {
                (it.requester as Query).soloExecutionTime()
            }
            val meanWaitingTimeFraction = values.sumOf {
                (it.released - it.requested) / (it.requester as Query).soloExecutionTime() - 1
            } / values.size
            val totalQueries = values.map { it.requester }.distinct().count()
            object {
                val BucketStart = bucket * bucketSize
                val BucketEnd = BucketStart + bucketSize
                val FractionOfTimeWaited = totalWaitingTime / totalExecutionTime
                val MeanWaitingTimeFraction = meanWaitingTimeFraction
                val TotalQueries = totalQueries //each query uses network twice: there and back
            }
        }
        .asDataFrame()
        .moveLeft("BucketStart", "BucketEnd", "TotalQueries")
    return df
}

fun extractStatsFromFibersActivityTimeline(
    fibersActivities: List<ResourceActivityEvent>,
    bucketSize: Int
): DataFrame {
    val df = fibersActivities.groupBy { it.time.value.toInt() / bucketSize }
        .map { (bucket, values) ->
            val totalQueries = values.map { it.requester }.distinct().count()
            val totalWaitingTime = values.sumOf {
                (it.released - it.requested) - (it.requester as Query).networkTime.toDouble()
            }
            val totalExecutionTime = values.sumOf {
                //each query uses network twice: there and back
                2 * (it.requester as Query).networkTime.toDouble()
            }
            val meanWaitingTimeFraction = values.sumOf {
                (it.released - it.requested) / (it.requester as Query).networkTime.toDouble()
            } / values.size

            val activitiesByFiber = values.groupBy { it.resource as Fiber }
            val statsByFiber = activitiesByFiber.map { (fiber, fiberActivities) ->
                val totalTimeInUse = fiberActivities.sumOf { it.released - it.honored }
                val totalTimeWaited = fiberActivities.sumOf { it.honored - it.requested }

                fiber to object {
                    val queriesServed = fiberActivities.distinctBy { it.requester }.count()
                    val fiberUtilization = totalTimeInUse * 1.0 / (bucketSize)
                    val meanWaitingTime = totalTimeWaited * 1.0 / totalTimeInUse
                }
            }
            val minUtilization = statsByFiber.minOf { (fiber, stats) -> stats.fiberUtilization }
            val maxUtilization = statsByFiber.maxOf { (fiber, stats) -> stats.fiberUtilization }
            val meanUtilization = statsByFiber.map { (fiber, stats) -> stats.fiberUtilization }.mean()
            val meanUtilizationSquares = statsByFiber.map { (fiber, stats) -> stats.fiberUtilization.pow(2) }.mean()
            val stdevUtilization = sqrt(meanUtilizationSquares - meanUtilization.pow(2))

            object {
                val BucketStart = bucket * bucketSize
                val BucketEnd = BucketStart + bucketSize
                val FractionOfTimeWaited = totalWaitingTime / totalExecutionTime
                val MeanWaitingTimeFraction = meanWaitingTimeFraction
                val TotalQueries = totalQueries //each query uses network twice: there and back
                val MinUtilization = minUtilization
                val MaxUtilization = maxUtilization
                val StdevUtilization = stdevUtilization
            }
        }.asDataFrame()
        .moveLeft("BucketStart", "BucketEnd", "TotalQueries")
    return df
}

data class FiberStats(
    val queriesServed: Int,

    val fiberUtilization: Double,
    val meanWaitingTime: Double,

    val connectionsGoingThrough: Int,
)

fun printFibersStats(
    fibers: Iterable<Fiber>,
    connections: Iterable<Connection>,
    timeRange: ClosedRange<TickTime>,
) {
    val statsByFiber = fiberStats(timeRange, fibers, connections)
    val totalQueries = statsByFiber.values.sumOf { it.queriesServed }
    val minUtilization = statsByFiber.minOf { (fiber, stats) -> stats.fiberUtilization }
    val maxUtilization = statsByFiber.maxOf { (fiber, stats) -> stats.fiberUtilization }
    val meanUtilization = statsByFiber.map { (fiber, stats) -> stats.fiberUtilization }.mean()
    val meanUtilizationSquares = statsByFiber.map { (fiber, stats) -> stats.fiberUtilization.pow(2) }.mean()
    val stdevUtilization = sqrt(meanUtilizationSquares - meanUtilization.pow(2))

    println(
        "Fibers load for ($timeRange), " +
                "utilization: [${minUtilization.toPercents()}%..${maxUtilization.toPercents()}%], " +
                "range ${(maxUtilization - minUtilization).toPercents()}%, stdev: ${stdevUtilization.toPercents()}%"
    )
    statsByFiber.forEach { (fiber, stats) ->
        println(
            "\tFiber[${fiber._name}]: served ${stats.queriesServed} queries (${(stats.queriesServed * 100.0 / totalQueries).toInt()}%), " +
                    "utilization ${(stats.fiberUtilization * 100).roundToInt()}%, " +
                    "mean waiting ${(stats.meanWaitingTime * 100).roundToInt()}%, " +
                    "connections remains in pool: ${stats.connectionsGoingThrough}"
        )
    }
}

fun fiberStats(
    timeRange: ClosedRange<TickTime>,
    fibers: Iterable<Fiber>,
    connections: Iterable<Connection>
): Map<Fiber, FiberStats> {
    val totalTime = with(timeRange) { endInclusive - start }

    val activitiesInTimeRangeByFiber = fibers.associateBy(
        { fiber -> fiber },
        { fiber ->
            fiber.activities
                .filter { it.requested in timeRange }
        }
    )

    val statsByFiber = activitiesInTimeRangeByFiber.map { (fiber, fiberActivities) ->
        val queriesServed = fiberActivities.distinctBy { it.requester }.count()
        val totalTimeInUse = fiberActivities.sumOf { it.released - it.honored }
        val totalTimeWaited = fiberActivities.sumOf { it.honored - it.requested }

        //Interestingly, totalTimeWaited expected to be highest for most utilized
        //       fibers, but it doesn't. Probably, it is because most of waiting time
        //       is collected during spikes, but most utilization is during regular
        //       traffic?
        fiber to FiberStats(
            connectionsGoingThrough = connections.count { it.goingThroughFiber == fiber },

            queriesServed = queriesServed,
            fiberUtilization = totalTimeInUse * 1.0 / totalTime,
            meanWaitingTime = if (queriesServed > 0) {
                totalTimeWaited * 1.0 / totalTimeInUse
            } else {
                0.0
            }
        )
    }.toMap()
    return statsByFiber
}


fun Double.roundTo(digits: Int): Double {
    if (this.isNaN()) {
        return this
    }
    val powed = 10.0.pow(digits.toDouble())
    return (this * powed).roundToInt() / powed;
}

fun Double.toPercents(): Int {
    return (this * 100).roundToInt()
}

