package ru.cheremin.mrupool

import krangl.*
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.kalasim.*
import java.io.File
import kotlin.math.abs
import kotlin.math.max
import kotlin.math.roundToInt
import kotlin.random.Random
import kotlin.test.Ignore


/**
 *
 * @author ruslan
 * created 08.04.2022 at 10:18
 */
internal class MultiFibersMRUPoolSimulationSandboxTest {

    /**If null -> print it to stdout*/
    private val simulationStatisticsCSVToFile = File("result.csv")

    private val TOTAL_SIMULATION_TIME = 20_000_000.0
    private val DEFAULT_ISSUE_BATCH_AT = 2_500_000.0

    private val TIMELINE_BUCKET_SIZE = (TOTAL_SIMULATION_TIME / 100).toInt()

    private val DEFAULT_IDLE_CONNECTION_TIMEOUT = 10_000.0

    private val DEFAULT_MEAN_DB_TIME = 5.0
    private val DEFAULT_MEAN_NETWORK_TIME = 11.0
    private val DEFAULT_NETWORK_UTILIZATION = 0.3


    @Test
    fun `MRU pool25 behavior without spikes of load`() {
        val fibersCount = 5
        val applicationsCount = 1

        val maxPoolSize = 25
        val queriesInBatch = maxPoolSize

        val (simulation, stats) = runSimulation(
            fibersCount = fibersCount,
            applicationsCount = applicationsCount,

            autoSizingPool = false,
            minPoolSize = maxPoolSize,
            maxPoolSize = maxPoolSize,

            expectedNetworkUtilization = DEFAULT_NETWORK_UTILIZATION,

            issueQueriesUntilTime = TOTAL_SIMULATION_TIME,
        )

        checkMetaStableIssueReproduced(simulation, stats, expectToReproduce = false)
    }

    @Test
    fun `shallow MRU pool behavior with 1 spike of load`() {
        val fibersCount = 5
        val applicationsCount = 1

        val maxPoolSize = 25
        val queriesInBatch = maxPoolSize

        val (simulation, stats) = runSimulation(
            fibersCount = fibersCount,
            applicationsCount = applicationsCount,

            autoSizingPool = false,
            minPoolSize = maxPoolSize,
            maxPoolSize = maxPoolSize,

            expectedNetworkUtilization = DEFAULT_NETWORK_UTILIZATION,

            issueQueriesUntilTime = TOTAL_SIMULATION_TIME,
        ) {
            val timeOfBatch = DEFAULT_ISSUE_BATCH_AT
            issueBatchOfQueriesToEachApplication(queriesInBatch, atTime = timeOfBatch)

            val timeToProcessBatch =
                queriesInBatch * (2 * DEFAULT_MEAN_NETWORK_TIME + DEFAULT_MEAN_DB_TIME) * applicationsCount
            PrintPoolComponent(
                at = (timeOfBatch + timeToProcessBatch + 1000).asTickTime(),
                simulation = this
            )
        }

        checkMetaStableIssueReproduced(simulation, stats, expectToReproduce = false)
    }

    @Test
    fun `MRU auto-sized pool behavior after few spikes`() {
        val fibersCount = 5
        val applicationsCount = 1

        val maxPoolSize = 100
        val queriesInBatch = maxPoolSize

        val (simulation, stats) = runSimulation(
            fibersCount = fibersCount,
            applicationsCount = applicationsCount,

            autoSizingPool = true,
            idleConnectionTimeout = DEFAULT_IDLE_CONNECTION_TIMEOUT,
            minPoolSize = maxPoolSize / 2,
            maxPoolSize = maxPoolSize,

            expectedNetworkUtilization = DEFAULT_NETWORK_UTILIZATION,

            issueQueriesUntilTime = TOTAL_SIMULATION_TIME,
        ) {
            for (i in 0..20) {
                issueBatchOfQueriesToEachApplication(
                    queriesInBatch,
                    atTime = DEFAULT_ISSUE_BATCH_AT + i * 2 * DEFAULT_IDLE_CONNECTION_TIMEOUT
                )
            }

            PrintPoolComponent(
                at = DEFAULT_ISSUE_BATCH_AT + 21 * (DEFAULT_IDLE_CONNECTION_TIMEOUT * 2),
                simulation = this
            )
        }

        checkMetaStableIssueReproduced(simulation, stats, expectToReproduce = true)
    }

    @Test
    fun `issue several spikes of load and print pool state after each`() {
        val fibersCount = 5
        val applicationsCount = 2

        val maxPoolSize = 100
        val queriesInBatch = maxPoolSize

        val (simulation, stats) = runSimulation(
            fibersCount = fibersCount,
            applicationsCount = applicationsCount,

            autoSizingPool = false,
            minPoolSize = maxPoolSize,
            maxPoolSize = maxPoolSize,

            expectedNetworkUtilization = DEFAULT_NETWORK_UTILIZATION,

//            meanInterArrivalTime = 10 * TOTAL_SIMULATION_TIME,

            issueQueriesUntilTime = TOTAL_SIMULATION_TIME,
        ) {
            for (i in 0..15) {
                val timeOfBatch = DEFAULT_ISSUE_BATCH_AT + i * 50_000
                issueBatchOfQueriesToEachApplication(queriesInBatch, atTime = timeOfBatch)

                val timeToProcessBatch =
                    queriesInBatch * (2 * DEFAULT_MEAN_NETWORK_TIME + DEFAULT_MEAN_DB_TIME) * applicationsCount
                PrintPoolComponent(
                    at = (timeOfBatch + timeToProcessBatch + 1000).asTickTime(),
                    simulation = this
                )
            }
        }

        //checkMetaStableIssueReproduced(simulation, stats, expectToReproduce = true)
    }

    @RepeatedTest(5)
    fun `meta-stable failure IS NOT reproduced with single application 700 connections in statically-sized MRU _by taking time_ pool`() {
        val poolSize = 700
        val queriesInBatch = poolSize

        val (simulation, stats) = runSimulation(
            fibersCount = 5,
            applicationsCount = 1,

            autoSizingPool = false,
            mruByTakingTime = true,
            maxPoolSize = poolSize,
            minPoolSize = poolSize,

            expectedNetworkUtilization = DEFAULT_NETWORK_UTILIZATION,

            issueQueriesUntilTime = TOTAL_SIMULATION_TIME,
        ) {
            issueBatchOfQueriesToEachApplication(queriesInBatch, atTime = DEFAULT_ISSUE_BATCH_AT)
        }
        checkMetaStableIssueReproduced(simulation, stats, expectToReproduce = false)
        //it is slightly worse than LRU pool, since MRU tries hard to use as little
        // connections as possible, and among those little there is high chance of
        // >1 connections going through same fiber.
        // It should be averaged in scenarios with many applications?
    }

    @RepeatedTest(5)
    fun `meta-stable failure IS NOT reproduced with 25 applications and 50 connections in statically-sized MRU _by taking time_ pool`() {
        val fibersCount = 5
        val applicationsCount = 25

        val poolSize = 50
        val queriesInBatch = poolSize

        val (simulation, stats) = runSimulation(
            fibersCount = fibersCount,
            applicationsCount = applicationsCount,

            autoSizingPool = false,
            mruByTakingTime = true,
            minPoolSize = poolSize,
            maxPoolSize = poolSize,

            expectedNetworkUtilization = DEFAULT_NETWORK_UTILIZATION,

            issueQueriesUntilTime = TOTAL_SIMULATION_TIME,
        ) {
            issueBatchOfQueriesToEachApplication(queriesInBatch, atTime = DEFAULT_ISSUE_BATCH_AT)
        }

        checkMetaStableIssueReproduced(simulation, stats, expectToReproduce = false)
    }



    //========================


    @RepeatedTest(5)
    fun `meta-stable failure is NOT reproduced with single application and 30 connections in pool`() {
        val fibersCount = 5

        val poolSize = 30
        val queriesInBatch = poolSize

        val reproductions = generateSequence {}.take(5).map {
            val (simulation, stats) = runSimulation(
                fibersCount = fibersCount,
                applicationsCount = 1,

                autoSizingPool = false,
                minPoolSize = poolSize,
                maxPoolSize = poolSize,

                expectedNetworkUtilization = DEFAULT_NETWORK_UTILIZATION,

                issueQueriesUntilTime = TOTAL_SIMULATION_TIME,
            ) {
                issueBatchOfQueriesToEachApplication(queriesInBatch, atTime = DEFAULT_ISSUE_BATCH_AT)
            }

            val metaStableIssueReproduced = checkMetaStableIssueReproduced(
                simulation,
                stats,
                expectToReproduce = false,
                assert = false
            )
            metaStableIssueReproduced
        }.toList()
        println("Meta-stable issue expected to NOT reproduced: ${reproductions}")
    }

    @RepeatedTest(5)
    fun `meta-stable failure IS reproduced with 25 applications and auto-sized pool`() {
        val fibersCount = 5
        val applicationsCount = 25

        val maxPoolSize = 40
        val queriesInBatch = maxPoolSize

        val (simulation, stats) = runSimulation(
            fibersCount = fibersCount,
            applicationsCount = applicationsCount,

            autoSizingPool = true,
            idleConnectionTimeout = DEFAULT_IDLE_CONNECTION_TIMEOUT,
            minPoolSize = fibersCount * 2,
            maxPoolSize = maxPoolSize,

            expectedNetworkUtilization = DEFAULT_NETWORK_UTILIZATION,

            issueQueriesUntilTime = TOTAL_SIMULATION_TIME,
        ) {
            issueBatchOfQueriesToEachApplication(queriesInBatch, atTime = DEFAULT_ISSUE_BATCH_AT)
        }

        checkMetaStableIssueReproduced(simulation, stats, expectToReproduce = true)
    }

    @RepeatedTest(5)
    fun `meta-stable failure IS reproduced with 25 applications and 50 connections in pool`() {
        val fibersCount = 5
        val applicationsCount = 25

        val poolSize = 50
        val queriesInBatch = poolSize

        val (simulation, stats) = runSimulation(
            fibersCount = fibersCount,
            applicationsCount = applicationsCount,

            autoSizingPool = false,
            minPoolSize = poolSize,
            maxPoolSize = poolSize,

            expectedNetworkUtilization = DEFAULT_NETWORK_UTILIZATION,

            issueQueriesUntilTime = TOTAL_SIMULATION_TIME,
        ) {
            issueBatchOfQueriesToEachApplication(queriesInBatch, atTime = DEFAULT_ISSUE_BATCH_AT)
        }

        checkMetaStableIssueReproduced(simulation, stats, expectToReproduce = true)
    }

    @RepeatedTest(5)
    @Ignore("Don't work as expected")
    fun `meta-stable failure IS NOT reproduced with 15 applications and high load`() {
        val fibersCount = 5
        val applicationsCount = 15

        val maxPoolSize = 100
        val queriesInBatch = maxPoolSize

        //high volume of random traffic quickly 'shuffles' pathological order of connections
        // created by spike load -- the higher the traffic, the faster shuffling is happening,
        // so instead of meta-stable network failure we'll get only short period of higher
        // network latency after the spike itself
        val expectedNetworkUtilization = 0.4

        val (simulation, stats) = runSimulation(
            fibersCount = fibersCount,
            applicationsCount = applicationsCount,

            autoSizingPool = true,
            idleConnectionTimeout = DEFAULT_IDLE_CONNECTION_TIMEOUT,
            minPoolSize = fibersCount * 2,
            maxPoolSize = maxPoolSize,

            expectedNetworkUtilization = expectedNetworkUtilization,

            issueQueriesUntilTime = TOTAL_SIMULATION_TIME,
        ) {
            issueBatchOfQueriesToEachApplication(queriesInBatch, atTime = DEFAULT_ISSUE_BATCH_AT)
        }
        checkMetaStableIssueReproduced(simulation, stats, expectToReproduce = false)
    }

    @RepeatedTest(5)
    fun `meta-stable failure IS reproduced with single application only with very deep 400 auto-sized pool`() {
        val fibersCount = 5
        val applicationsCount = 1

        val maxPoolSize = 400
        val queriesInBatch = maxPoolSize

        val (simulation, stats) = runSimulation(
            fibersCount = fibersCount,
            applicationsCount = applicationsCount,

            autoSizingPool = true,
            idleConnectionTimeout = DEFAULT_IDLE_CONNECTION_TIMEOUT,
            minPoolSize = maxPoolSize / 2,
            maxPoolSize = maxPoolSize,

            expectedNetworkUtilization = DEFAULT_NETWORK_UTILIZATION,

            issueQueriesUntilTime = TOTAL_SIMULATION_TIME,
        ) {
            issueBatchOfQueriesToEachApplication(queriesInBatch, atTime = DEFAULT_ISSUE_BATCH_AT)
        }

        checkMetaStableIssueReproduced(simulation, stats, expectToReproduce = true)
    }

    @RepeatedTest(5)
    fun `meta-stable failure IS reproduced with single application only with very deep 700 statically-sized pool`() {
        val poolSize = 700
        val queriesInBatch = poolSize

        val (simulation, stats) = runSimulation(
            fibersCount = 5,
            applicationsCount = 1,

            autoSizingPool = false,
            maxPoolSize = poolSize,
            minPoolSize = poolSize,

            expectedNetworkUtilization = DEFAULT_NETWORK_UTILIZATION,

            issueQueriesUntilTime = TOTAL_SIMULATION_TIME,
        ) {
            issueBatchOfQueriesToEachApplication(queriesInBatch, atTime = DEFAULT_ISSUE_BATCH_AT)
        }
        checkMetaStableIssueReproduced(simulation, stats, expectToReproduce = true)
    }

    //======================= infra ====================================================

    private var currentTestInfo: TestInfo? = null

    @BeforeEach
    fun rememberTestName(testInfo: TestInfo) {
        println(testInfo.displayName)
        currentTestInfo = testInfo
        testInfo.displayName.replace(' ', '_')
    }

    private fun MRUPoolSimulation.issueBatchOfQueriesToEachApplication(
        queriesInBatch: Int,
        atTime: Double
    ) {
        for ((index, pool) in pools.withIndex()) {
            //Single 'login' query: batch of queries to use all the pool
            object : Component(at = (atTime + index * 5000).asTickTime()) {
                override fun process() = sequence<Component> {
                    for (i in 1..queriesInBatch) {
                        Query(
                            pool,
                            dbTime = dbTimePD.sample(),
                            networkTime = networkTimePD.sample()
                        )
                    }
                }
            }
        }
    }


    private fun checkMetaStableIssueReproducedBak(
        simulation: MRUPoolSimulation,
        stats: DataFrame,
        expectToReproduce: Boolean,
    ) {
        val lastTenth = (TOTAL_SIMULATION_TIME * 9 / 10).asTickTime()..TOTAL_SIMULATION_TIME.asTickTime()
        val fibersWithStats = fiberStats(
            lastTenth,
            simulation.fibers,
            simulation.pools.flatMap { it.connections }
        )
        simulation.fibers.map { fiber ->
            val lastSegmentActivity = fiber.activities.filter { it.requested in lastTenth }
            val totalTimeInUse = lastSegmentActivity.sumOf { it.released - it.honored }
            val totalTimeWaited = lastSegmentActivity.sumOf { it.honored - it.requested }
            object {
                val totalTimeInUse = totalTimeInUse
                val totalTimeWaited = totalTimeWaited
            }
        }

        val totalQueriesServed = fibersWithStats.values.sumOf { it.queriesServed }
        val (mostOccupiedFiber, mostOccupiedFiberStats) = fibersWithStats.maxByOrNull { (fiber, stats) ->
            stats.queriesServed
        }!!


        val queriesFor100Utilization =
            fibersWithStats.values.sumOf { stats ->
                if (stats.queriesServed > 0) {
                    stats.queriesServed / stats.fiberUtilization
                } else {
                    0.0
                }
            }
        val averageUtilization = totalQueriesServed / queriesFor100Utilization

        //This is quite hard to define when 'meta-stable issue' is present. It manifests
        // itself as most of the load converged to a single fiber, which is overloaded.
        // So natural criterion is (percent of queries served by most occupied fiber is
        // above some threshold) AND (most occupied fiber is utilized heavily enough)
        // But it is hard to define 'above some threshold', and 'heavily enough': e.g.
        // if overall utilization is 80%, then _all_ fibers are heavily utilized in any
        // case, regardless of the m-s issue. Contrary, if overall utilization is 5%, then
        // even all load being converged to a single fiber doesn't create high utilization.
        // I decided to address that by defining meta-stable issue as one fiber processes
        // twice the average amount of traffic, and its utilization is above 75%, or 10%
        // above average utilization, depends on which is higher

        val thresholdMostOccupiedFiberServed = 2 * (1.0 / fibersWithStats.size)
        val thresholdMostOccupiedFiberUtilization = max(0.75, averageUtilization + 0.1)

        val queriesFractionServedByMostOccupiedFiber = mostOccupiedFiberStats.queriesServed * 1.0 / totalQueriesServed

        val metaStableIssueIsLikelyReproduced =
            (queriesFractionServedByMostOccupiedFiber >= thresholdMostOccupiedFiberServed
                    && mostOccupiedFiberStats.fiberUtilization >= thresholdMostOccupiedFiberUtilization)
        if (expectToReproduce) {
            assertThat(
                "Expect issue to be meta-stable, i.e.: " +
                        "most occupied fiber served ${queriesFractionServedByMostOccupiedFiber.toPercents()}% >= ${thresholdMostOccupiedFiberServed.toPercents()}% of all queries " +
                        "AND utilization ${mostOccupiedFiberStats.fiberUtilization.toPercents()}% >= ${thresholdMostOccupiedFiberUtilization.toPercents()}%",
                metaStableIssueIsLikelyReproduced,
                equalTo(true)
            )
        } else {
            assertThat(
                "Expect issue to be NOT meta-stable, i.e.: " +
                        "most occupied fiber served ${queriesFractionServedByMostOccupiedFiber.toPercents()}% < 1/2 of all queries " +
                        "OR utilization ${mostOccupiedFiberStats.fiberUtilization.toPercents()}% < 80%",
                metaStableIssueIsLikelyReproduced,
                equalTo(false)
            )
        }
    }

    private fun checkMetaStableIssueReproduced(
        simulation: MRUPoolSimulation,
        stats: DataFrame,
        expectToReproduce: Boolean,
        assert: Boolean = true
    ): Boolean {
        val firstTenth = 0.0.asTickTime()..(TOTAL_SIMULATION_TIME / 10).asTickTime()
        val lastTenth = (TOTAL_SIMULATION_TIME * 9 / 10).asTickTime()..TOTAL_SIMULATION_TIME.asTickTime()
        val totalTimeInUseBefore = simulation.fibers.sumOf { fiber ->
            fiber.activities.filter { it.requested in firstTenth }
                .sumOf { it.released - it.honored }
        }
        val totalTimeWaitedBefore = simulation.fibers.sumOf { fiber ->
            fiber.activities.filter { it.requested in firstTenth }
                .sumOf { it.honored - it.requested }
        }
        val totalTimeInUseAfter = simulation.fibers.sumOf { fiber ->
            fiber.activities.filter { it.requested in lastTenth }
                .sumOf { it.released - it.honored }
        }
        val totalTimeWaitedAfter = simulation.fibers.sumOf { fiber ->
            fiber.activities.filter { it.requested in lastTenth }
                .sumOf { it.honored - it.requested }
        }


        //This is quite hard to define when 'meta-stable issue' is present. It manifests
        // itself as most of the load converged to a single fiber, which is overloaded.
        // So natural criterion is (percent of queries served by most occupied fiber is
        // above some threshold) AND (most occupied fiber is utilized heavily enough)
        // But it is hard to define 'above some threshold', and 'heavily enough': e.g.
        // if overall utilization is 80%, then _all_ fibers are heavily utilized in any
        // case, regardless of the m-s issue. Contrary, if overall utilization is 5%, then
        // even all load being converged to a single fiber doesn't create high utilization.
        // I decided to address that by defining meta-stable issue as one fiber processes
        // twice the average amount of traffic, and its utilization is above 75%, or 10%
        // above average utilization, depends on which is higher


        val fractionTimeWaitedBefore = totalTimeWaitedBefore / totalTimeInUseBefore
        val fractionTimeWaitedAfter = totalTimeWaitedAfter / totalTimeInUseAfter
        val metaStableIssueIsLikelyReproduced =
            (fractionTimeWaitedAfter >= fractionTimeWaitedBefore * 2)
                    && (fractionTimeWaitedAfter > 0.5)
        //todo AND >50% queries processed by 1 fiber?
        println(
            "Fraction of time waited: " +
                    "${fractionTimeWaitedBefore.roundTo(2)} before, ${fractionTimeWaitedAfter.roundTo(2)} after -> " +
                    "${
                        if (metaStableIssueIsLikelyReproduced) {
                            "reproduced"
                        } else {
                            "not reproduced"
                        }
                    }"
        )
        if (assert) {
            if (expectToReproduce) {
                assertThat(
                    "Expect issue to be meta-stable, i.e. fraction of time waited is twice up, and >0.5 after the spike: " +
                            "${fractionTimeWaitedBefore.roundTo(2)} -> ${fractionTimeWaitedAfter.roundTo(2)}",
                    metaStableIssueIsLikelyReproduced,
                    equalTo(true)
                )
            } else {
                assertThat(
                    "Expect issue to be NOT meta-stable, i.e fraction of time waited is <twice up, or <0.5 after the spike: " +
                            "${fractionTimeWaitedBefore.roundTo(2)} -> ${fractionTimeWaitedAfter.roundTo(2)}",
                    metaStableIssueIsLikelyReproduced,
                    equalTo(false)
                )
            }
        }
        return expectToReproduce
    }

    fun runSimulation(
        randomSeed: Int = Random.nextInt(),

        fibersCount: Int,
        applicationsCount: Int,

        autoSizingPool: Boolean,
        mruByTakingTime: Boolean = false,
        idleConnectionTimeout: Double = Double.MAX_VALUE,
        maxPoolSize: Int,
        minPoolSize: Int,

        meanNetworkTime: Double = DEFAULT_MEAN_NETWORK_TIME,
        meanDbTime: Double = DEFAULT_MEAN_DB_TIME,
        expectedNetworkUtilization: Double = DEFAULT_NETWORK_UTILIZATION,
        meanInterArrivalTime: Double = (meanNetworkTime * 2.0) * applicationsCount / fibersCount / expectedNetworkUtilization,

        issueQueriesUntilTime: Double,

        init: MRUPoolSimulation.() -> Unit = {},
    ): Pair<MRUPoolSimulation, DataFrame> {
        if (!autoSizingPool) {
            check(minPoolSize == maxPoolSize) { "minPoolSize($minPoolSize) must be equal to maxPoolSize($maxPoolSize) for !autoSizingPool" }
        }
        val poolFactory: MRUPoolSimulation.() -> Pool = if (autoSizingPool) {
            {
                var connectionIndex = 0;
                val randomSeed = random.nextInt()
                MRUPoolAutoSizing(
                    {
                        val fiberIndex = abs((connectionIndex++) + randomSeed) % fibers.size
                        val fiber = fibers[fiberIndex]
                        Connection("conn[$connectionIndex] via $fiber", fiber)
                    },
                    idleConnectionTimeout = idleConnectionTimeout,
                    maxSize = maxPoolSize,
                    minSize = minPoolSize
                )
            }
        } else {
            {
                var connectionIndex = 0;
                val randomSeed = random.nextInt()
                val connectionFactory = {
                    val fiberIndex = abs((connectionIndex++) + randomSeed) % fibers.size
                    val fiber = fibers[fiberIndex]
                    Connection("conn[$connectionIndex] via $fiber", fiber)
                }
                if (mruByTakingTime) {
                    PoolWithCustomizableOrder(
                        connectionFactory,
                        poolSize = maxPoolSize,
                        PoolWithCustomizableOrder.MOST_RECENTLY_TAKEN
                    )
                } else {
                    PoolWithCustomizableOrder(
                        connectionFactory,
                        poolSize = maxPoolSize,
                        PoolWithCustomizableOrder.MOST_RECENTLY_RELEASED
                    )
//                    MRUPool(
//                        connectionFactory,
//                        poolSize = maxPoolSize
//                    )
                }
            }
        }

        println(
            "Scenario:\n" +
                    "   network fibers: $fibersCount\n" +
                    "   applications: $applicationsCount\n" +
                    "   queries: \n" +
                    "       mean inter-arrival time: $meanInterArrivalTime ms\n" +
                    "       path (ms): network: $meanNetworkTime -> db: $meanDbTime -> network: $meanNetworkTime"
        )
        if (autoSizingPool) {
            println("   pool(s): auto-sizing $minPoolSize..$maxPoolSize, idle timeout: $idleConnectionTimeout ms")
        } else {
            if (mruByTakingTime) {
                println("   pool(s): static size: $maxPoolSize, MRU by taking time")
            } else {
                println("   pool(s): static size: $maxPoolSize")
            }
        }

        val poolUtilization = (meanNetworkTime * 2.0 + meanDbTime) / meanInterArrivalTime
        val networkUtilization = (meanNetworkTime * 2.0) * applicationsCount / meanInterArrivalTime / fibersCount
        println(
            "   expected utilization:\n" +
                    "       per pool: ~${(poolUtilization * 100).roundToInt()}%\n" +
                    "       network:  ~${(networkUtilization * 100).roundToInt()}%"
        )
        println(
            "Run for ${issueQueriesUntilTime}ms: \n" +
                    "   ..."
        )


        val simulation = MRUPoolSimulation(
            randomSeed,
            fibersCount,
            applicationsCount,
            meanInterArrivalTime,
            meanNetworkTime,
            meanDbTime,
            issueQueriesUntilTime,
            poolFactory = poolFactory
        )
        simulation.init()

        simulation.run()

        // ==== print results:
        val totalSimulatedTime =
            simulation.fibers.maxOf { it.activities.maxOf { it.released } } - simulation.fibers.minOf { it.activities.minOf { it.requested } }
        val totalNetworkTime = simulation.fibers.sumOf { fiber ->
            fiber.activities.sumOf { it.released - it.honored }
        }
        val realNetworkUtilization = totalNetworkTime / totalSimulatedTime / fibersCount
        println(
            "Simulation finished: \n" +
                    "   total time simulated: ${totalSimulatedTime.roundToInt()} ms\n" +
                    "   real network utilization: ${(realNetworkUtilization * 100).roundToInt()}%"
        )

        val allConnections = simulation.pools.flatMap { it.connections }

        //print fiber stats for first 10% and last 10% of simulation timeframe:
        printFibersStats(
            simulation.fibers,
            allConnections,
            (0.asTickTime())..((issueQueriesUntilTime / 10).asTickTime())
        )
        printFibersStats(
            simulation.fibers,
            allConnections,
            ((issueQueriesUntilTime * 9.0 / 10).asTickTime())..(issueQueriesUntilTime.asTickTime())
        )

        //Print (processing time/raw processing time) averaged per bucket:
        val poolTimelineStats = extractStatsFromPoolActivityTimeline(
            simulation.pools.flatMap { it.activities },
            bucketSize = TIMELINE_BUCKET_SIZE
        )
        val fibersTimelineStats = extractStatsFromFibersActivityTimeline(
            simulation.fibers.flatMap { it.activities },
            bucketSize = TIMELINE_BUCKET_SIZE
        )

        val totalStatsDF = poolTimelineStats
            .innerJoin(fibersTimelineStats, by = "BucketStart", suffices = "OnPool" to "OnFiber")
            .remove("BucketEndOnFiber")
            .rename("BucketEndOnPool" to "BucketEnd")


        if (simulationStatisticsCSVToFile != null) {
            totalStatsDF.writeCSV(simulationStatisticsCSVToFile)
            currentTestInfo?.let {
                val name = it.displayName.replace(' ', '_').replace("[)(]".toRegex(), "") + ".csv"
                totalStatsDF.writeCSV(
                    File(name),
                )
            }
        } else {
            totalStatsDF.print(
                title = "History",
                rowNumbers = false,
                maxRows = Int.MAX_VALUE,
                maxWidth = Int.MAX_VALUE
            )
        }

        return simulation to totalStatsDF
    }

    class MRUPoolSimulation(
        randomSeed: Int = Random.nextInt(),
        fibersCount: Int,
        applicationsCount: Int,
        meanInterArrivalTime: Double,
        meanNetworkTime: Double,
        meanDbTime: Double,
        issueQueriesUntilTime: Double,
        poolFactory: MRUPoolSimulation.() -> Pool
    ) : Environment(randomSeed = randomSeed) {
        val fibers = List(fibersCount) { i ->
            //break symmetry: make some fibers slightly slower than others
            Fiber("$i", delay = i * 0.5 / fibersCount)
        }

        val pools: List<Pool> = List(applicationsCount) {
            this.poolFactory()
        }

        val interArrivalPD = exponential(meanInterArrivalTime)

        val networkTimePD = constant(meanNetworkTime)
        val dbTimePD = constant(meanDbTime)

        init {
            for (pool in pools) {
                //'Regular' queries, one-by-one:
                ComponentGenerator(iat = interArrivalPD, until = TickTime(issueQueriesUntilTime)) {
                    Query(
                        pool,
                        dbTime = dbTimePD.sample(),
                        networkTime = networkTimePD.sample()
                    )
                }
            }
        }
    }

    class PrintPoolComponent(at: TickTime, val simulation: MRUPoolSimulation) : Component(at = at) {
        constructor(at: Double, simulation: MRUPoolSimulation) : this(at.asTickTime(), simulation)

        override fun process() = sequence<Component> {
            //TODO why there is no clustering in multi-app setup after spike load?
            println("Pools at ${simulation.now}:")
            for ((index, pool) in simulation.pools.withIndex()) {
                println("   pool ${index}: ${pool.connections}")
                println("   ${formatSliced2(pool.connections, 20)}")
            }
        }
    }
}

fun formatSliced(
    connections: Iterable<Connection>,
    groupSize: Int
): String {
    return connections.mapIndexed { index, conn -> index to conn }
        .groupBy({ it.first / groupSize }, { it.second })
        .map { it.key * groupSize to it.value.sumOf { it.goingThroughFiber.delay.toDouble() } }
        .joinToString(prefix = "\t", separator = "\n\t") {
            "${it.first}: totalDelay ${it.second}"
        }
}

fun formatSliced2(
    connections: Iterable<Connection>,
    groupSize: Int
): String {
    val fibers = connections.map { it.goingThroughFiber }
        .distinct()
        .sortedBy { it.name }

    return connections.mapIndexed { index, conn -> index to conn }
        .groupBy({ it.first / groupSize }, { it.second.goingThroughFiber })
        .map { (group, fibersInGroup) ->
            val fibersCount = fibers.map { fiber ->
                fibersInGroup.count { it === fiber }
            }
            group * groupSize to fibersCount
        }
        .joinToString(prefix = "\t", separator = "\n\t") {
            "${it.first}..${it.first + groupSize - 1}: fibers ${it.second.joinToString()}"
        }
}