Simulation of "link imbalance metastable issue" described in 
 * ["Metastable Failures in Distributed Systems"](https://doi.org/10.1145/3458336.3465286) Nathan Bronson, Abutalib Aghayev, Aleksey Charapko, Timothy Zhu
 * ["Solving the Mystery of Link Imbalance: A Metastable Failure State at Scale"](https://engineering.fb.com/2014/11/14/production-engineering/solving-the-mystery-of-link-imbalance-a-metastable-failure-state-at-scale/)
 
Simulation is built on [kalasim](https://www.kalasim.org/) -- Kotlin discrete-events simulation engine.

### Link imbalance metastable issue 
Basically, issue arises in facebook datacenters due to interaction between:
 * MRU strategy in DB connection pools, used by apps
 * Multi-fiber network link between apps and DB server
 * Periodical spikes of load of many small short queries

These 3 pieces together interact in such a way that almost all network activity is attracted
to a single fiber, which exceeds its capacity.

More details:
 * ["Metastable Failures in Distributed Systems"](https://doi.org/10.1145/3458336.3465286) Nathan Bronson, Abutalib Aghayev, Aleksey Charapko, Timothy Zhu
 * ["Solving the Mystery of Link Imbalance: A Metastable Failure State at Scale"](https://engineering.fb.com/2014/11/14/production-engineering/solving-the-mystery-of-link-imbalance-a-metastable-failure-state-at-scale/)


### Simulation structure
Simulation is build around `Pool`, `Fiber`, `Connection` and `Query` domain objects.
"Client" (which is the source of `Query`es) is modelled by `kalasim`'s `ComponentGenerator`
"DB" behavior in current simulation is just a delay, hence it is not modelled explicitly at all.
                                                         
Both `Pool` implementation and `Fiber` are extending `kalasim` `Resource`, which gives
acquire/release occupation-controlling behavior for free, but also allows to rely on `kalasim`
statistical data gathered for `Resource`s
                                                             
Simulation primitives are in `MultiFibersMRUPoolSimulation`, and different scenarios
are grouped in tests, e.g. `MultiFibersMRUPoolSimulationScenarios`. These tests are 
probabilistic by nature, i.e. success/failure is usually not guaranteed, but happens
with high enough probability, e.g. >90%. Many tests are used `RepeatedTest` annotation
to give a sense of success/failure odds explicitly. 
