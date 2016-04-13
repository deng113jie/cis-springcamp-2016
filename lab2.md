<div role="main"><span id="maincontent"></span><h2>CIS Spring Camp - Lab 2 -Graph Processing</h2><div class="box generalbox center clearfix"><div class="no-overflow"><p>In this second lab we are going to use the GraphX extension of Spark to load and process large-scale graphs. Remember that Spark keeps all the loaded datasets and transformations in memory, which might exceed the capacity of the machines we are using. For this lab we have selected smaller samples of the full datasets we have collected, in order to avoid these problems. </p>
<p></p>
<p>We will use the same command line shell from the previous lab (spark-shell). In order to use GraphX functionality we first need to import its libraries, we can do that by typing the following command:</p>
<pre>import org.apache.spark.graphx._</pre>
<p></p>
<h3>Creating Graphs with GraphX</h3>
<p>In order to work with GraphX we need to create a Graph RDD that represents both the structure of the graph, and the associated properties to the edges and vertices. We will create the graph by loading from the HDFS an edge list file (that contains one edge per line, in the format (source destination). Spark graphs are directed graphs (so relationships are not symmetrical). They are also multigraphs by default, which means there might be multiple different edges with the same source and destination.</p>
<pre>//generate graph from edge dataset. The graph used in this example may takes up to 10 minutes to load into Spark<br /><br />val followerGraph = GraphLoader.edgeListFile(sc, "hdfs://moonshot-ha-nameservice/camp/follows.name.160113.2015.hash")</pre>
<p>This graph only contains the raw node and edges structure, with vertices identified by long numbers. We will now extend the structure to provide the name of each user and channel to its respective vertex as an attribute. We can use for that a second file that contains the mapping of long ids to channel and user names.<br /><br /></p>
<pre>val names = sc.textFile("hdfs://moonshot-ha-nameservice/camp/names.map").filter(x=&gt;x.split(" ").length==2).map(line =&gt; (line.split(" ")(1).<span>toInt</span>, line.split(" ")(0) ))<br /><br />val newatt = followerGraph.vertices.map(x=&gt;(x._2,x._1)).join(names).map(x=&gt;((x._2)._1,  (((x._2)._2),x._1)) )<br /><br />val namegraph = followerGraph.outerJoinVertices(newatt){ case (id, name, nameid ) =&gt; ( name, nameid) }.cache</pre>
<p></p>
<p>The outerJoinVertices generates a new type of graph by join the current vertices RDD with the new RDD, newatt is this case. The newatt here contains the id(Int) of the streamer and the name(String), use newatt.first to check.</p>
<p>We invoke cache in this graph as we will use it Remember how Spark is a lazy framework: no transformation will be executed until an action is requested. We can now start accessing our graph structure. The following actions compute the size of the graph, in both nodes and edges.</p>
<pre>namegraph.vertices.count()<br />namegraph.edges.count()</pre>
<p>Q: Here the type of new graph generated is "org.apache.spark.graphx.Graph[(Int, Option[(String, Int)]),Int] ", can you recognize which type is which attributes?</p>
<p>Q: Also, the Option[(String, Int)]) is a little cumbersome here, is it possible to define a new class to encapsulate both attributes?</p>
<p>hint:</p>
<pre>case class User(name: String, id:Int)<br />val namegraph = followerGraph.outerJoinVertices(newatt){ case ( id, nameid1, name) =&gt; User( name.get._1 , nameid1) }</pre>
<h3>Degree exploration</h3>
<p>We can use some of the implemented methods of GraphX to compute the degree distribution of each vertex. The inDegrees and outDegrees transformations return pairs of elements, with the first element being the id of the vertex, and the second element the degree count. We will use the stats() method of Spark to compute some statistical properties of the overall degree distribution.</p>
<div>
<pre>followerGraph.inDegrees.map(x =&gt; x._2).stats()<br />followerGraph.outDegrees.map(x =&gt; x._2).stats()</pre>
<p>This exploration performs computation on the distribution of degree values. If we want to find the vertex representing the channel with the highest number of followers, we will need to define a different transformation. This time we will use the reduce action, to send back to the driver a single result, containing the vertex id with the maximum indegree count:</p>
</div>
<div>
<pre>// Define a reduce operation to compute the highest degree vertex<br />def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {<br />  if (a._2 &gt; b._2) a else b<br />}<br />// Compute the max degrees<br />val maxInDegree: (VertexId, Int)  = graph.inDegrees.reduce(max)</pre>
<p></p>
<h3>Graph enrichment</h3>
<p>We can combine multiple datasets, in order to provide additional information into the graph we have generated. In this section we will obtain the maximum viewership figures of each channel, as collected in the dataset we used yesterday, and add that information to each vertex as the second attribute (first one is the channel name).</p>
<pre>// it would be good if we make sure this is computed in the first lab, and saved as a file, so that we can just...<br />// Retrieve the max degrees in RDD names stored or compute then from yesterdays examples<br />val namesMax = // complete your code here or look at the code below<br />// make sure your key (first element) is the name of the channel, so that you can do the join later</pre>
<p>We now need to use an outer join because only channels appear in yesterday/s dataset (not users).</p>
<pre>val newatt2 = newgraph.vertices.map(x=&gt;((x._2)._1,x._1,x_.3)).join(namesMax).map(// complete to have VertexId type as the key)<br /><br /></pre>
<pre>val augmentedgraph = newgraph.outerJoinVertices(names).cache()</pre>
<p><strong>Exercise : </strong></p>
<p>Among all the channels with 100 or more followers, compute the ratio of maximum viewers divided by the number of followers. Get the distribution statistics, and the maximum value analogously to what we did before with the in degrees.  </p>
<pre> </pre>
<p></p>
</div>
<div></div>
<div></div>
<div>
<h3>Graph creation</h3>
</div>
<div>Graphs are no more than an abstraction to represent relationships. In this part of the lab we are going to evaluate the similarity of channels depending on whether users are mutual followers of some of these channels. We will evaluate data the following way: For each user u, who follows channels (c1, c2, ...., cn), we will identify graph edges among all the channels in the follower set. That is, if user u follows channels c1, c2, and c3, the following edges will be generated: c1-c2, c1-c3, c2-c3. When processing all the users, we will obtain repeated edges this way, so we should consolidate these repeated edges. We will do that by adding the number of common edges as an attribute of the created one. </div>
<p></p>
<pre>val userFollowers = sc.textFile(sc, "hdfs://moonshot-ha-nameservice/camp/&lt;&lt;&lt;lab1maxviewers&gt;&gt;&gt;").map(line = &gt; (line.split("\t")._1.toLong, line.split("\t")._2.toLong) )<br />val edges = edges.map(e =&gt; (e,1) ).reduceByKey( (a,b)=&gt; a+b).map(x=&gt; Edge(x._1._1, x._1._2, x._2) )<br />//missing making it directed. <br /><code><span class="kwd">val</span><span class="pln"> channelGraph </span><span class="pun">:</span><span class="pln"> </span><span class="typ">Graph</span><span class="pun">[Int</span><span class="typ"></span><span class="pun">]</span><span class="pln"> </span><span class="pun">=</span><span class="pln"> </span><span class="typ">Graph</span><span class="pun">.</span><span class="pln">fromEdges</span><span class="pun">(</span><span class="pln">edges.union(edges.reverse()</span><span class="pun">,</span><span class="pln"> 1</span><span class="pun">).cache()</span></code></pre>
<p>We have used a new method to create the graph. This time, we created an RDD of Edge elements, with sourceId, destinationId, and property(being an Integer, with the number of users with the same). We have created the graph as a directed one this time, by transforming the set of edges into its uniion with the same set reversed. </p>
<p>Spend some time familiarising with this new graph. Using the transformations you learned previously in the lab, find out the number of nodes, edges (will be half of the provided value), and distribution of degrees. </p>
<p>We can use this graph to compute further graph analysis methods, such as the PageRank coefficient. Compute PageRank on this new graph, who is the most central user according to these metrics?</p>
<pre><code class="language-scala" data-lang="scala"><span class="c1"><span class="k">val</span><span> </span><span class="n">ranks</span><span> </span><span class="k">=</span><span> new</span><span class="n">graph</span><span class="o">.</span><span class="n">pageRank</span><span class="o">(</span><span class="mf">0.0001</span><span class="o">).</span><span class="n">vertices<br /></span><br />// try to see what ranks is like<br />ranks.take(10)<br />// we can join the vertexID in the first attribute of ranks with the vertexIDs from followerGraph to join the vertices each with their rank</span><span class="k"><br /><br /></span><span class="o"></span></code></pre>
<h3>Bonus 1: Implement custom graph algorithms using Pregel-style computations</h3>
<h3></h3>
<p>All the graph operations performed in this lab sheet have used methods predefined by GraphX. There are additional methods we haven't covered, which can be seen in the documentation. However, there are numerous computations that need to be implemented manually. For this purpose, GraphX exposes a Pregel interface where custom computations can be implemented. For doing so we can use the .pregel() transformation, that executes three functions iteratively.</p>
<p>a) Update vertex state<br />b) Merge messages<br />c) Send messages<br />These algorithms also require to initialise the state of the graph in many cases, as each computation will update the state of the vertex.<br /><br /></p>
<p></p>
<p>import org.apache.spark.graphx.lib._</p>
<pre>channelGraph.joinVertices(to initialise values)<br />channelGraph.pregel(with the three functions)</pre>
<pre></pre>
<p>def pregel[A](initialMsg: A, maxIterations: Int, activeDirection: EdgeDirection)(<br /> vprog: (VertexID, VD, A) =&gt; VD,<br /> sendMsg: EdgeTriplet[VD, ED] =&gt; Iterator[(VertexID,A)],<br /> mergeMsg: (A, A) =&gt; A)<br /> : Graph[VD, ED]</p>
<p></p>
