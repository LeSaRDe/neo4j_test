# Alternative Thoughts About Processing Queries on Large Scale Networks

## Motivation

We are seeking more efficient methods in solving queries on contact networks and simulation results produced by EpiHiper than those that need to load in the entire time series of contact networks and create full indexes on node/edge properties. And, if possible, we want to generalize these methods to arbitrary networks. In addition, we also want to understand how much and what information is required to fulfill different types of queries, and when only partial information is provided whether we could attain acceptable approximate query results. 

Note that sometimes the intermediate contact networks produced by EpiHiper are called `transmission trees`. To avoid confusion, we reserve the name `initial contact network` to the input contact network for EpiHiper, and we call those intermediate networks produced by EpiHiper `intermediate contact networks` or more briefly `intermediate networks`. 

Beforehand, let us review why graph DBMS or relational DBMS would take a long time at the loading and indexing stages. 

**(1) Neo4j: Too Many Edge Copies** 

When using Neo4j, since Neo4j does not support friendly array-like data types for node/edge properties which can be indexed, it becomes difficult for us to stuff the temporal information (i.e. time stamps of edges) into a single edge property while keeping executing temporal queries efficiently. 

Limited by this issue, we trade off the space consumption, and load in all edges in all contact networks into the Neo4j DB. And each edge is associated with a simple integer property indicating the time point at which this edge occurs. Doing this enables us bypass the difficulty of using array-like data types for storing the temporal information on edges. And an index is created on that integer edge property. 

However, this method significantly increases the total number of edges involved in the indexing on edge properties. And, in fact, for the edge properties other than the time stamp, it may not be necessary to create indexes on all edges over time. Rather, an index for each time point can be sufficient for queries. 

**(2) Eliminating Edge Copies May Not Solve the Problem**

First, when temporal networks are stored a database, creating an index on an edge property will usually take into account at least all unique edges, which, again, may not be necessary for fulfilling queries. 

Second, usually indexes are created individually, and B-tree data structures are used. Constructing a B-tree index can cost $O(n t \log_t n)$, where $n$ is the size of data and $t$ is the minimum degree of the B-tree. Also, sorting the data beforehand benefits insertions, which also $O(n \log n)$. Empirically, sorting an edge file containing 1.4 billion records takes about 1 hour on a Rivanna computing node (Intel Xeon Gold 6148 @ 2.40GHz x 40 threads, 376GB memory). In other words, for each edge property, creating an index may take a few hours. And, when the number of edge properties requiring indexing increases, the running time of creating all indexes may be prolonged due to the limits of computing resources. 

To sum up, no matter graph databases or relational databases are in use, it is likely that we may not be able to gain any solution with satisfactory running time for data loading and indexing without utilizing distributed computing resources and advanced parallelization. 

On the other hand, it is also interesting to us what queries can be acceptably fulfilled with limited computing resources, if any. In other words, the following question is asked:

> **what types of queries do not need all information of the entire time series of contact networks?**

More specifically, the queries, in the worst case, that can be fulfilled without the need of accessing every node and edge in all contact networks are wanted.

Let us take a look at a counter example first: 

> What are PIDs of individuals who were infected by individuals whose ages are between 21 and 45?

For a specific simulation run, the results for this query may not include the entire set of nodes at each simulated time point; however, in the worst case, at each simulated time point, every individual may have been infected by the individuals whose ages are between 21 and 45, and thus every node and edge in all contact networks are needed to produce the results. This type of queries is not in our primary consideration. 

Now, let us modify the above query to the following:

> What is the age distribution of individuals who were infected by individuals whose ages are between 21 and 45?

To provide an acceptable answer to this question, in the worst case, do we have to consider every node and edge in all contact networks? Intuitively, first, we can sample from the individuals whose ages are between 21 and 45, second, we can further sample from the infected individuals, and finally, we compute an approximate age distribution based on the samples. If the resulting approximate distribution is acceptably close to the desired/empirical distribution, then we say that we do not have to consider every node and node in all contact networks. 

From these two examples, we learned that:
- **Queries inquiring about distributions** may be acceptably fulfilled by accessing only a limited portion of data. 
- Sampling is a key in delivering acceptable results. 
- Evaluating the quality of query results is an important problem.

Note that other types of queries that do not require all information of the entire time series of contact networks may also exist. And we are in discovery of other possibilities. 

**Note for Caution**

Identifying classes of queries that do not need the full information of the time series of contact network does not necessarily mean that in general we would be able to process queries more efficient than using DBMS. When doing sampling, the full information may be also needed depending on what types of sampling is performed. For example, when sampling from imbalanced data, clustering information is usually needed, which may require sorting. In this case, if all node/edge properties are considered in queries, then the overall running time may not be better than that using DBMS, if not worse, because it may need a sorting on each property. On the other hand, if a sampling method with a lower time complexity than $O(n \log n)$ can fulfill a non-trivial class of queries acceptably and it only needs to be run once, then this class is outstanding. 


## Queries Inquiring About Distributions (QuID)

This is a class of queries.

> **Definition**: **QuID**
> A QuID query requires a distribution as its desired result.

It is necessary to understand what are the items upon which the desired distributions are computed. And the following is a collection (may not be the full list):
- Node properties (e.g. age and gender)
- Edge properties (e.g. activity type and duration)
- Properties of graph structures (e.g. degree and shortest path length)

Note that it may not be true that we can find an acceptable approximate result for every QuID query. As mentioned before, the quality of an approximate result needs to be evaluated by the distance/divergence between this approximate result and the desired/empirical result. If the distance/divergence falls into a predetermined range of acceptance, the approximate result is said to be *accepted*. 

Also note that there are quite a number of candidates for the distance/divergence such as $f$-divergence, JS-divergence, Bhattacharyya distance, Wasserstein distance and energy distance. It is necessary to clarify which distance/divergence is in use when accepting an approximate result. 

> **Definition**: **QuID-AAR**
> We define a sub-class of QuID in which each member query corresponds to at least one acceptable approximate result w.r.t. at least one distance/divergence with a predetermined acceptance range. And we name this sub-class **QuID with Acceptable Approximate Results (QuID-AAR)**. In addition, for clarification, a member in QuID-AAR should be said to be **$\alpha$-$\Phi$-accepted**, where $\alpha$ is the value of distance/divergence and $\Phi$ is the name of the distance/divergence (in abbr.) (e.g. $0.1$-Wasserstein-accepted). 

Note that QuID itself is a QuID-AAR if the acceptance range is arbitrary. 

We are also interested in how the values of distance/divergence worsens when the size of samples for output decreases regardless of the sampling methods and how the final result is computed. In other words, for each QuID query, we seek a function to reflect such relationships: 

> **Definition**: **Sample-Size Approximation-Quality Function (SSAQF)**
> $\rho: |\hat{G}| \mapsto \inf\limits_{\varphi, \hat{G}} d(\varphi(\hat{G}), f)$, where $\hat{G}$ is a sample of the time series of contact networks, $\varphi$ is a method computing the approximate result, $f$ is the desired/empirical distribution and $d$ denotes the selected distance/divergence. 

SSAQFs of QuID queries may have significantly different characteristics (e.g. linear, sublinear, superlinear, quadratic, exponential and etc.). These characteristics provide an approach to measure the "hardness" of the queries.

In addition, seeking a SSAQF is an optimization problem. Fully solving this problem can be difficult, and thus imposing constraints to this problems may be necessary. 


## Sampling for QuID

As mentioned above, node properties, edge properties and graph structure properties are three typical items on which distributions may be desired. This also means that these items can also appear as conditions of given queries. Thus, to fulfill a QuID query, the sampling may be performed multiple times on different items. 

For queries exclusively requiring node/edge properties, when multiple properties are involved, a sampling from the multivariate distribution is needed. In this case, the multivariate data can be imbalanced and clustering information may be needed (i.e. one or multiple sorting may be needed and each costs $O(n \log n)$). Sampling from multivariate distributions is an art of its own. And it can be done more efficiently only if some special structures or correlations exist among the dimensions. 

On the other hand, sampling on graph structures is less straightforward (e.g. sampling shortest paths). Such sampling may need to be indirectly performed on nodes and edges. Let us take a look at an example. 

> We want to retrieve the distribution of shortest path lengths.

Suppose the sampling is performed on edges uniformly. In this case, the expected length of shortest paths in the sample may be lower than that in the original graph, and thus this sampling would result in a biased estimator, which may lead to misleading results. 

Seeking appropriate sampling methods on graph structures is a non-trivial problem, especially when the graph model of networks is unknown.


## Mapping From Queries to Graph Properties

Since nodes and edges can have arbitrary numbers of properties, it may be helpful if the relationships between sampling on the properties using specific methods and the variation of some graph properties. Such relationships can help cluster queries into groups. And it would be more generic studying the variation of graph properties and the variance of the quality of approximate results. 



<!-- We denote a path the following: 

> $l = \{(v_0, v_1), (v_1, v_2), \dots, (v_{n-1}, v_n)\}$

which contains $n$ nodes and $n-1$ edges. To sample on paths, it is necessary to know the probability of any given path. We denote the probability of an edge the following: $P(e_{i,j} \in \mathcal{E} | v_i, v_j)$, which is a prior given the end nodes $v_i$ and $v_j$. And we use the notation $P(e_{ij} | v_i, v_j)$ for simplicity. By the Bayes' theorem, 

> $P(e_{i,j} | v_i, v_j) = \frac{P(v_i, v_j | e_{i,j}) P(e_{i,j})}{P(v_i, v_j)}$

where $P(e_{i,j})$ is the prior probability of edges. 

Note that it always holds that 

> $P(v_i, v_j | e_{i,j}) = 1$

Also, we assume that a node will be automatically removed if it is not incident to any edge, then:

> $\begin{aligned}
    P(v_i, v_j) &= \sum\limits_{\forall e_z \in \mathcal{E}} P(v_i, v_j | e_z) P(e_z)\\
    &= \sum\limits_{\forall e_z \in \mathcal{N}(v_i) \setminus \{e_{i,j}, e_{j,i}\} } P(v_i | e_z) P(e_z) P(v_j) 
    + \sum\limits_{\forall e_w \in \mathcal(N)(v_j) \setminus \{e_{i,j}, e_{j,i}\} } P(v_j | e_w) P(e_w) P(v_i) \\
    &+ \sum\limits_{\forall e_o \notin \mathcal{N}(v_i) \cup \mathcal(N)(v_j)} P(v_i, v_j | e_o) P(e_o) + P(v_i, v_j | e_{i,j}) P(e_{i,j}) + P(v_i, v_j | e_{j, i}) P(e_{j, i})
\end{aligned}$

where $\mathcal{N}(v_i)$ denotes the set of incident edges of $v_i$.

Note that $v_i$ is independent of $v_j$ given the neighborhood of $v_i$ excluding the edges $e_{i,j}$ and $e_{j, i}$. The probability of nodes can be determined by the probabilities of its incident edges.

> $\begin{aligned}
    P(v_i) &= \sum\limits_{\forall e_z \in \mathcal{E}} P(v_i | e_z) P(e_z)\\
    &= \sum\limits_{\forall e_z \in \mathcal{N}(v_i)} P(v_i | e_z) P(e_z) 
    + \sum\limits_{\forall e_w \notin \mathcal{N}(v_i)} P(v_i | e_w) P(e_w)
\end{aligned}$

Also note that it holds that

> $P(v_i | e_z) = 1$

if $v_i$ is incident to $e_z$.

Hence, we have

> $P(v_i) = \sum\limits_{\forall e_z \in \mathcal{N}(v_i)} P(e_z) + \sum\limits_{\forall e_w \notin \mathcal{N}(v_i)} P(v_i) P(e_w)$

And, it is induced
 > $P(v_i) = \frac{\sum\limits_{\forall e_z \in \mathcal{N}(v_i)} P(e_z)}{1 - }$

Hence, 
> $P(v_i, v_j) = \sum\limits_{\forall e_z \in \{e_{i,a}\}|_a \cup \{e_{b,i}\}|_b \setminus \{e_{i,j}, e_{j,i}\}} P(e_z) \sum\limits_{\forall e_w \in \{e_{j,a}\}|_a \cup \{e_{b,j}\}|_b} P(e_w) + \sum\limits_{\forall e_p \in \{e_{j,a}\}|_a \cup \{e_{b,j}\}|_b \setminus \{e_{i,j}, e_{j,i}\} } P(e_p) \sum\limits_{\forall e_q \in \{e_{i,a}\}|_a \cup \{e_{b,i}\}|_b} P(e_q) + P(e_{i,j}) + P(e_{j,i})$

where $P(e_z)$'s are also prior probabilities of edges.

Then, we have:

> $P(e_{i,j} | v_i, v_j) = \frac{P(e_{i, j})}{\sum\limits_{\forall e_z \in \{e_{i,a}\}|_a \cup \{e_{b,i}\}|_b \setminus \{e_{i,j}, e_{j,i}\}} P(e_z) \sum\limits_{\forall e_w \in \{e_{j,a}\}|_a \cup \{e_{b,j}\}|_b} P(e_w) + \sum\limits_{\forall e_p \in \{e_{j,a}\}|_a \cup \{e_{b,j}\}|_b \setminus \{e_{i,j}, e_{j,i}\} } P(e_p) \sum\limits_{\forall e_q \in \{e_{i,a}\}|_a \cup \{e_{b,i}\}|_b} P(e_q) + P(e_{i,j}) + P(e_{j,i})}$

If we also assume that edges are independent of each other, then the probability of a path is as follows:

> $\begin{aligned}
    P(l | v_0, v_1, \dots, v_n) &= \prod\limits_{\substack{\forall e_{i, j} \in l \\ v_i, v_j \in e_{i,j}}} P(e_{i,j} | v_i, v_j)\\
    &= \prod\limits_{\substack{\forall e_{w, z} \in l \\ v_w, v_z \in e_{w,z}}} \frac{P(e_{i, j})}{
        \begin{aligned}
        &\sum\limits_{\forall e_z \in \{e_{i,a}\}|_a \cup \{e_{b,i}\}|_b \setminus \{e_{i,j}, e_{j,i}\}} P(e_z) \sum\limits_{\forall e_w \in \{e_{j,a}\}|_a \cup \{e_{b,j}\}|_b} P(e_w)\\
        &+ \sum\limits_{\forall e_p \in \{e_{j,a}\}|_a \cup \{e_{b,j}\}|_b \setminus \{e_{i,j}, e_{j,i}\} } P(e_p) \sum\limits_{\forall e_q \in \{e_{i,a}\}|_a \cup \{e_{b,i}\}|_b} P(e_q)\\
        &+ P(e_{i,j}) + P(e_{j,i})
        \end{aligned}}
\end{aligned}$

Suppose that $P(e_{i,j})$ is uniform, and we denote the uniform probability by $P(e)$ then 

> $\begin{aligned}
    P(e_{i,j} | v_i, v_j) &\approx \frac{P(e)}{[\deg_{in}(v_i) + \deg_{out}(v_i)][\deg_{in}(v_j) + \deg_{out}(v_j)]P^2(e) + [\deg_{in}(v_j) + \deg_{out}(v_j)][\deg_{in}(v_i) + \deg_{out}(v_i)]P^2(e) + 2P(e)}\\
    &= \frac{1}{2 \Big([\deg_{in}(v_i) + \deg_{out}(v_i)][\deg_{in}(v_j) + \deg_{out}(v_j)]P(e) + 1 \Big)}
\end{aligned}$

Note that the approximation comes from $e_{i,j}$ and $e_{j,i}$ being not considered in the denominator.

And hence,

> $P(l | v_0, v_1, \dots, v_n) = \prod\limits_{\substack{\forall e_{i, j} \in l_k \\ v_i, v_j \in e_{i,j}}} \frac{1}{2 \Big([\deg_{in}(v_i) + \deg_{out}(v_i)][\deg_{in}(v_j) + \deg_{out}(v_j)]P(e) + 1 \Big)}$

The expected length of paths then is computed as

> $\mathbb{E}\Big[ m P(l_m | v_0, v_1, \dots, v_m) \Big] = \sum\limits_{0 < m \leq n} \sum\limits_{\substack{\forall e_{i, j} \in l_m \\ v_i, v_j \in e_{i,j}}} \frac{m}{2 \Big([\deg_{in}(v_i) + \deg_{out}(v_i)][\deg_{in}(v_j) + \deg_{out}(v_j)]P(e) + 1 \Big)}$

It can be observed that if we sample from edges  -->


<!-- On the other hand, we claim that a type of queries that may not require all information of the entire time series of contact networks is the class *inquiring about distributions*. More precisely, for a query seeking a distribution up to a set of given conditions, if the distribution induced from a collection of subgraphs of the contact networks is acceptably close to the desired distribution, then we say that this query does not necessarily need all information of the entire time series of contact networks. This class of queries is in our primary consideration. However, this class is difficult to be rigorously defined because the criteria for "accceptably close" may vary case by case. For this reason, instead, we consider the class of queries which inquire about distributions and the results can be approximated by operations on small subgraphs. For each query in this class, the distance (or at least divergence) between the desired distribution and the approximate distribution needs to be computed, and the distance/divergence is a measure of the extent to which the query depends on the entire time series. And we name this class **the class of approximate distribution queries (ADQ)**.  -->




<!-- ## ADQ

#### **Informal Definition**

A query is said to be in the ADQ class when it satisfies the following conditions:
- The desired result of this query should be a distribution (univariate or multivariate);
- To obtain the best result (i.e. an empirical distribution taking all data involved by this query into account), one or multiple contact networks (including both the initial network and intermediate networks) in a simulation need to be accessed;
- To obtain the best result, at least one of the following items needs to be accessed when accessing the contact networks: nodes, edges, node properties and edge properties. 
- The result can be approximated by accessing limited amount of contact networks as well as nodes, edges, node properties and edge properties therein. 
- There is an effective method computing the distance, or divergence, between the desired result (or the best result) and the approximate result. 

#### **Metrics**

In the informal definition of ADQ, two aspects catch our top attention:
- How much data is accessed in approximating the result. 
- The distance/divergence between the desired/best result and the approximate result.

W.r.t. the first aspect, we may straightforwardly use proportions to measure. 

W.r.t. the second aspect, there is quite a selection of distances/divergences for comparing two distributions such as $f$-divergence, JS-divergence, Bhattacharyya distance, Wasserstein distance and energy distance. These distances/divergences have pros and cons, and may be suitable for different scenarios. Choosing distances/divergences is an art of its own. 

One of most interesting problems related to the above two aspects is their correlations. Specifically, we would like to understand how the distances/divergences vary along with the changes in the amount of data being accessed. 

However, since the ingredients involved in a query (i.e. nodes, edges, node properties and edge properties) can be arbitrary, it may be difficult or inefficient to fully understand the correlations. Furthermore, the studies on such correlations could hardly be generalized to arbitrary definitions of networks (beyond the contact networks operated by EpiHiper). 

Alternatively, if we can establish connections between the changes in the amount of accessed data and graph properties, then studying the correlations between these graph properties and the distances/divergences will become more generic. This idea arises a key problem.

> **Problem 1**
> What graph properties can be used to capture the changes in the amount of accessed data? -->