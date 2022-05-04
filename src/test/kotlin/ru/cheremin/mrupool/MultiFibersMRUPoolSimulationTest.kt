package ru.cheremin.mrupool

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.equalTo
import org.hamcrest.Matchers.not
import org.junit.jupiter.api.Test
import org.kalasim.ComponentGenerator
import org.kalasim.Environment
import org.kalasim.constant
import org.kalasim.exponential
import kotlin.math.exp
import kotlin.math.pow
import kotlin.math.roundToInt
import kotlin.random.Random


const val DEFAULT_POOL_SIZE: Int = 9
const val DEFAULT_FIBERS_COUNT: Int = 3

const val DEFAULT_REGULAR_QUERIES_COUNT: Int = 20_000 // how many 'regular' queries to issue

/**
 *
 * @author ruslan
 * created 06.04.2022 at 13:55
 */
internal class MultiFibersMRUPoolSimulationTest {

    @Test
    fun withQueriesComingRegularlySpacedWaitingTimesAreAllZeroesAndConnectionsAreNotReorderedInPool() {
        //Sanity check for simulation code: D/D/N system should have 0 waiting time,
        // (if utilization <100%), and only single channel should be used, if utilization
        // is <=1/N
        val meanDbTime = 5
        val meanNetworkTime = 7

        //make utilization close to 100% FOR A SINGLE CONNECTION (not for all POOL_SIZE/FIBERS_COUNT)
        //     (I.e. real utilization will be 1/FIBERS_COUNT)
        val meanInterArrivalTime = (meanNetworkTime * 2.0 + meanDbTime)

        val utilization = (meanNetworkTime * 2.0 + meanDbTime) / meanInterArrivalTime / DEFAULT_FIBERS_COUNT
        println(
            "Utilization = ${(utilization * 100).roundToInt()}%\n" +
                    "\tmeanIAT=$meanInterArrivalTime\n" +
                    "\tnetwork=$meanNetworkTime -> db=$meanDbTime -> network=$meanNetworkTime"
        )

        val simulation = MRUSimulation(fibersCount = DEFAULT_FIBERS_COUNT, poolSize = DEFAULT_POOL_SIZE).apply {
            val interArrivalPD = constant(meanInterArrivalTime)
            val networkTimePD = constant(meanNetworkTime)
            val dbTimePD = constant(meanDbTime)

            //'Regular' queries, one-by-one:
            ComponentGenerator(iat = interArrivalPD, total = DEFAULT_REGULAR_QUERIES_COUNT) {
                Query(
                    pool,
                    dbTime = dbTimePD.sample(),
                    networkTime = networkTimePD.sample()
                )
            }
        }

        val connectionsBeforeSimulation = ArrayList(simulation.pool.connections)
        simulation.run()
        val connectionsAfterSimulation = ArrayList(simulation.pool.connections)

        assertThat(
            "Connections shouldn't be reordered in pool, since only 1 top connection was really in use",
            connectionsBeforeSimulation,
            equalTo(connectionsAfterSimulation)
        )

        val totalWaitingTime = simulation.pool.activities.sumOf {
            (it.released - it.requested) - (it.requester as Query).soloExecutionTime()
        }
        assertThat(
            "Should be no waiting time with utilization<=100% and regularly spaced requests",
            totalWaitingTime,
            equalTo(0.0)
        )
    }

    @Test
    fun withSpikingLoadConnectionsInMRUPoolAreReorderedSoSlowestConnectionClimbsOnTheTop() {
        //Put spike of load with poolSize queries, in other aspects equal to each other.
        //  If some connections in pool are slower than others, after a spike connections
        //  in pool should be sorted from slowest to fastest (really this is 'sleep
        //  sort' in play)

        val meanDbTime = 5
        val meanNetworkTime = 7

        val poolSize = DEFAULT_FIBERS_COUNT
        val queriesInBatch = poolSize   //batch should use all connections in pool

        val simulation = MRUSimulation(
            fibersCount = DEFAULT_FIBERS_COUNT,
            poolSize = poolSize
        ).apply {
            val networkTimePD = constant(meanNetworkTime)
            val dbTimePD = constant(meanDbTime)
            //issue single batch of queries, to use all connections in pool:
            repeat(queriesInBatch) {
                Query(
                    pool,
                    dbTime = dbTimePD.sample(),
                    networkTime = networkTimePD.sample()
                )
            }
        }

        val connectionsBeforeSimulation = ArrayList(simulation.pool.connections)
        simulation.run()
        val connectionsAfterSimulation = ArrayList(simulation.pool.connections)


        assertThat(
            "Connections should be reordered in pool",
            connectionsAfterSimulation,
            not(equalTo(connectionsBeforeSimulation))
        )
        assertThat(
            "Connections should be reordered in pool so slowest one is in the top",
            connectionsAfterSimulation,
            equalTo(
                connectionsBeforeSimulation
                    .sortedByDescending { it.goingThroughFiber.delay.toDouble() }
            )
        )
    }

    @Test
    fun `poisson traffic shuffles _all_ pool connections eventually`() {
        //Observation: 'spike' load of many queries with same duration effectively
        //  'sleep sorts' connections in MRU pool. And random traffic does opposite:
        //  it effectively shuffles connections in MRU pool. But it takes time to shuffle
        //  _all_ connections, and here I try to estimate how long: how many queries to
        //  issue to 'touch' even deepest connection in the pool with high enough probability,
        //
        //To re-order _all_ pool connections it should at least be the moment when all
        //  connections are _in use_. I.e. it should be a moment with >= poolSize queries
        //  in progress.
        //  Probability of using N connections at least once during time T is approximated
        //  by
        //      P[N][t<=T] ~= 1-exp( -(1-rho)rho^(N-1) lambda T)
        //      lambda*T = E[queries arrived]
        //
        //  (Really in the test I check that the deepest connection is _re-ordered_,
        //  not just touched -- but since queries are all of constant duration,
        //  re-ordering happens almost always, and re-ordering not touching is
        //  that I really care about here, because it is re-ordering that leads to
        //  shuffling eventually -> what is why I test for re-ordering)
        //

        val meanDbTime = 0
        val meanNetworkTime = 7

        val poolSize = 10
        val meanFibersOccupied = 2.0
        val queriesCount = 5_000

        val meanInterArrivalTime = (meanNetworkTime * 2.0) / meanFibersOccupied

        val utilization = (meanNetworkTime * 2.0) / meanInterArrivalTime / DEFAULT_FIBERS_COUNT
        println(
            "Utilization (network) = ${(utilization * 100).roundToInt()}%\n" +
                    "\tmeanIAT=$meanInterArrivalTime\n" +
                    "\tnetwork=$meanNetworkTime -> db=$meanDbTime -> network=$meanNetworkTime\n" +
                    "\tP[shuffling] ~= ${p(utilization, poolSize, queriesCount)}"
        )

        val simulation = MRUSimulation(
            fibersCount = DEFAULT_FIBERS_COUNT,
            poolSize = poolSize
        ).apply {
            val interArrivalPD = exponential(meanInterArrivalTime)
            val networkTimePD = constant(meanNetworkTime)
            val dbTimePD = constant(meanDbTime)

            //'Regular' queries, one-by-one:
            ComponentGenerator(iat = interArrivalPD, total = queriesCount) {
                Query(
                    pool,
                    dbTime = dbTimePD.sample(),
                    networkTime = networkTimePD.sample()
                )
            }
        }

        val connectionsBeforeSimulation = ArrayList(simulation.pool.connections)
        simulation.run()
        val connectionsAfterSimulation = ArrayList(simulation.pool.connections)

        val deepestConnectionInitially = connectionsBeforeSimulation.last()
        val deepestConnectionAfterSimulation = connectionsAfterSimulation.last()
        assertThat(
            "Last (deepest) connection in pool should be moved by random traffic",
            deepestConnectionInitially,
            not(equalTo(deepestConnectionAfterSimulation))
        )
    }


    @Test
    fun `with low utilization shuffling under poisson traffic is very slow`() {
        // As stated in previous test, re-ordering of N connections in pool under
        // random (poisson) traffic happens with probability approximately
        //      P(N connections re-ordered) ~= exp(-A) * A^N/(N!)
        //  where A =~ (2*meanNetworkTime/meanInterArrivalTime), is how many network
        //  connections are in use _on average_, same as 'occupied fibers' expected
        //  value (if not all fibers are exhausted).
        //
        //  This probability decreases very fast with N, so if pool is big enough,
        //  reshuffling is almost never happens. It also decreases with low traffic
        //  i.e. low connections/fibers occupancy. Previous test uses A(average fibers
        //  occupied)=2, and poolSize = 10, which requires ~1e5 queries to re-order
        //  the deepest connection in the pool. Here I cut traffic 4 times, hence A=0.5
        //  hence it would require >1e10 queries to reliable re-order last connection
        //  in pool, so 1e5 must be not enough by a large margin

        val meanDbTime = 0
        val meanNetworkTime = 7

        val poolSize = 10
        val meanFibersOccupied = 0.4
        val queriesCount = 2_000_000

        val meanInterArrivalTime = (meanNetworkTime * 2.0) / meanFibersOccupied

        val utilization = (meanNetworkTime * 2.0) / meanInterArrivalTime / DEFAULT_FIBERS_COUNT
        println(
            "Utilization (network) = ${(utilization * 100).roundToInt()}%\n" +
                    "\tmeanIAT=$meanInterArrivalTime\n" +
                    "\tnetwork=$meanNetworkTime -> db=$meanDbTime -> network=$meanNetworkTime\n" +
                    "\tP[shuffling] ~= ${p(utilization, poolSize, queriesCount)}"
        )

        val simulation = MRUSimulation(
            fibersCount = DEFAULT_FIBERS_COUNT,
            poolSize = poolSize
        ).apply {
            val interArrivalPD = exponential(meanInterArrivalTime)
            val networkTimePD = constant(meanNetworkTime)
            val dbTimePD = constant(meanDbTime)

            //'Regular' queries, one-by-one:
            ComponentGenerator(iat = interArrivalPD, total = queriesCount) {
                Query(
                    pool,
                    dbTime = dbTimePD.sample(),
                    networkTime = networkTimePD.sample()
                )
            }
        }

        val connectionsBeforeSimulation = ArrayList(simulation.pool.connections)
        simulation.run()
        val connectionsAfterSimulation = ArrayList(simulation.pool.connections)

        val deepestConnectionInitially = connectionsBeforeSimulation.last()
        val deepestConnectionAfterSimulation = connectionsAfterSimulation.last()
        assertThat(
            "Last (deepest) connection in pool should NOT moved by low random traffic:" +
                    "\nbefore $connectionsBeforeSimulation" +
                    "\nafter $connectionsAfterSimulation",
            deepestConnectionInitially,
            equalTo(deepestConnectionAfterSimulation)
        )
    }


    @Test
    fun `with many connections in pool regular poisson traffic shuffling is very long`() {
        // As stated in previous test, re-ordering of N connections in pool under
        //  random (poisson) traffic happens probability decreasing very fast with
        //  N, so if pool is big enough, reshuffling is almost never happens. Previous
        //  tests use poolSize = 10, here I use poolSize=40 which requires >1e14 queries
        //  to reliable re-order last connection in pool, so 1e6 must be not enough by
        //  a large margin

        val meanDbTime = 0
        val meanNetworkTime = 7

        val poolSize = 40
        val meanFibersOccupied = 1.5
        val queriesCount = 1_000_000

        val meanInterArrivalTime = (meanNetworkTime * 2.0) / meanFibersOccupied

        val utilization = (meanNetworkTime * 2.0) / meanInterArrivalTime / DEFAULT_FIBERS_COUNT
        println(
            "Utilization (network) = ${(utilization * 100).roundToInt()}%\n" +
                    "\tmeanIAT=$meanInterArrivalTime\n" +
                    "\tnetwork=$meanNetworkTime -> db=$meanDbTime -> network=$meanNetworkTime\n" +
                    "\tP[shuffling] ~= ${p(utilization, poolSize, queriesCount)}"
        )

        val simulation = MRUSimulation(
            fibersCount = DEFAULT_FIBERS_COUNT,
            poolSize = poolSize
        ).apply {
            val interArrivalPD = exponential(meanInterArrivalTime)
            val networkTimePD = constant(meanNetworkTime)
            val dbTimePD = constant(meanDbTime)

            //'Regular' queries, one-by-one:
            ComponentGenerator(iat = interArrivalPD, total = queriesCount) {
                Query(
                    pool,
                    dbTime = dbTimePD.sample(),
                    networkTime = networkTimePD.sample()
                )
            }
        }

        val connectionsBeforeSimulation = ArrayList(simulation.pool.connections)
        simulation.run()
        val connectionsAfterSimulation = ArrayList(simulation.pool.connections)

        val deepestConnectionInitially = connectionsBeforeSimulation.last()
        val deepestConnectionAfterSimulation = connectionsAfterSimulation.last()
        assertThat(
            "Last (deepest) connection in pool should NOT moved by low random traffic:" +
                    "\nbefore $connectionsBeforeSimulation" +
                    "\nafter $connectionsAfterSimulation",
            deepestConnectionInitially,
            equalTo(deepestConnectionAfterSimulation)
        )
    }

    //TODO more realistic case there fibers=5, pool=50(fibers*10), and question is:
    //     how probable is to have top 10/20 connections in pool remain on top? I.e.
    //     bottom 30 connections untouched? How long it takes for this state to
    //     'decay' into a more regular 'shuffled' state?

    // Let us consider realistic case: fibers=5, pool=50(fibers*10). Regular load
    //     is 1.5 fiber, i.e. 30%. During spike of 100 queries top 10 connections in
    //     the pool becomes the worst, next 10 is not-so-good. Now it takes 1/0.3^10 ~= 400
    //     (really 5000) queries to reshuffle top 10 connections, and 1/0.3^10 ~= 150 000
    //     to reshuffle top 20 (really 50mln!)


    // ==================================================================================

    internal class MRUSimulation(
        randomSeed: Int = Random.nextInt(),
        val fibersCount: Int = DEFAULT_FIBERS_COUNT,
        val poolSize: Int = DEFAULT_POOL_SIZE,
    ) : Environment(randomSeed = randomSeed) {
        val fibers = List(fibersCount) { i ->
            //break symmetry: make some fibers slightly slower than others
            Fiber("$i", delay = i * 0.2 / fibersCount)
        }

        var connectionIndex = 0
        val pool = MRUPool(
            {
                val i = connectionIndex++
                val fiberIndex = i % fibers.size
                val fiber = fibers[fiberIndex]
                Connection("conn[$i] via $fiber", fiber)
            },
            poolSize
        )
    }

    private fun p(rho: Double, N: Int, queriesCount: Int): Double {
        //Probability (approximately) of using N-th connection in pool at least once,
        // during processing of queriesCount queries with utilization=rho
        val pN_1 = (1 - rho) * rho.pow(N - 1.0)
        //FIXME: p[0] = 1-rho -> OK
        //       p[1] = (1-rho)*rho -> NOT OK, it is rho!
        return 1 - exp(-pN_1 * queriesCount * 2)
    }
}