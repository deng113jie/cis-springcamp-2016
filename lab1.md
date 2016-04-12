 <div role="main"><span id="maincontent"></span><h2>CIS Spring Camp - Lab 1 - Big Data</h2><div class="box generalbox center clearfix"><div class="no-overflow"><p></p>
<h2>HDFS exploration</h2>
<p>In order to efficiently process large files, all the datasets for these labs are already stored in a distributed filesystem ran at the same machines we will use for computation (15  nodes). The</p>
<p>From the command line, you can browse and download the contents of that distributed filesystem using the command hadoop fs. The following webpage has some examples on how to check the contents of the distributed filesystem. </p>
<p></p>
<p>Check home folder, check data folder. </p>
<p></p>
<p>hadoop fs </p>
<p></p>
<p></p>
<h2>Introduction</h2>
<p>For this lab we will interact with Spark using exclusively the interactive Spark Shell, and writing commands in Scala. This shell works as a CLI (Command Line Interpreter) of Scala instructions.</p>
<p>You can start the spark shell by invoking from the ITL the command <span style="font-family: 'courier new', courier, monospace;"><strong>spark-shell</strong></span>. After several lines of debugging information, you will see a message prompt such as </p>
<pre>scala&gt;</pre>
<p>The shell creates a Scala Context object, sc, that can be invoked in order to create the initial RDDs for your computation. </p>
<p></p>
<p>There is an equivalent version that uses the Python, known as pyspark. For details on the python API for Spark, you can check the <a href="http://spark.apache.org/docs/latest/programming-guide.html">official documentation of Spark</a>.</p>
<p></p>
<h3>RDD creation and exploration with actions</h3>
<p>We will create our first RDD by reading the dataset from our lab 2 in Scala (the project Gutenberg files). In order to do so, we need to invoke the textFile method from the sc, and save the resulting RDD in a Scala variable (val):</p>
<pre>val reads= sc.textFile("hdfs://moonshot-ha-nameservice/camp/twitch.aug.txt")</pre>
<p>As you can see, in order to load HDFS files we need to specify the url of the HDFS namenode (in our case moonshot-ha-nameservice resolves to it within QMUL network).</p>
<p>Once the command finishes you will see a reference to the RDD. However, the RDD object does not exist yet. Spark RDDs are only materialised whenever we request a result from them. In order to do so, we will use one action, that returns some information from the RDD to our driver program. This involves network transfer, and loads the memory of the single driver machine. Let's invoke the count action on the RDD, which will return to the driver the number of elements in the RDD.</p>
<pre>reads.count()</pre>
<p>You will see several lines in the command line showing the operations triggered by your command, including the actual creation of the RDD from HDFS. You will finally see the result of your count action in the console. Additionally, by looking at the previous lines, you should be able to get some insight on what is happening in the cluster because of your command.</p>
<ul>
<li><em>How many entries appear in the Twitch 2015 dataset?</em></li>
</ul>
<ul>
<li><em>Based on what you can find from the logs, how many splits does the 'reads' RDD have? Remember this structure will be distributed among multiple nodes of your cluster.val</em></li>
</ul>
<p>You can check the number of partitions of an RDD by :</p>
<pre><code class="lang-scala">reads.partitions.size</code></pre>
<p>In order to have a look at a fraction of the RDD we can use a different action. takeSample collects a random sample of elements from the requested RDD. The following line will provide 10 entries back to the driver program. </p>
<pre>val sample = reads.takeSample(false, 10)</pre>
<p>When running the takeSample command you will see that again a set of transformations have been triggered.</p>
<ul>
<li><em>Compare the output of this operation with the one of the count before. Based on what you see, do you think the lines RDD is shared between two operations, or is it created again each time?</em></li>
</ul>
<p>Note that sample is a <strong>local </strong>variable inside the driver program. We can write any Scala code to manipulate it, and obtain any result from it.  For example, we can use <strong><span style="font-family: 'courier new', courier, monospace;">sample.foreach(println)</span></strong>, to print in the screen each element as a line of text. This is another example of Scala's functional programming approach (executing the provided function for each element from the array). As this is a local call, the code will execute immediately, no job needs to be scheduled in the cluster.</p>
<h3>Dataset selection and formatting</h3>
<p>The data comes initially in a TSV (Tab Separated Values) format, whereas we will be interested in a limited set of fields. A natural way is to map each line of the original dataset so that we only use the fields we are interested in. Scala offers us tuples of elements as a convenient way to store the number of values we are interested in. The following example selects from each measurement only the audience and game figures</p>
<p>For reference, the fields for each entry of the dataset represent the following information.:</p>
<p><em><strong>timestamp(integer)  number_of_current_viewers  total_views_of_channel  game_name   channel_creation_time   streamer_id  user_create_time  language  partner?(boolean) delay number_of_followers</strong></em></p>
<p>for example, here's an example of such a line :</p>
<p><strong>1420291196      14835   6337669    "League of Legends"     "2015-01-03T09:06:12Z"  "sneakycastroo"   "2011-09-02T23:16:11Z"   "ru"    true      0       152659</strong></p>
<p>First, and in almost every data collection process, the data is not clean due to different reasons; failures in the collection process, bugs, etc.</p>
<p>Let's filter the data first, that means only getting the data that is compliant to the format above,</p>
<pre><code>val freads = reads.filter(x=&gt;x.split("\t").length==11)</code></pre>
<p>We can also apply more complicated filters, for example, look for channels with old streamers subscribed before 2012</p>
<pre><code>freads.filter(x =&gt; x.split("\t")(6).substring(1,5).toInt&lt;2012)</code></pre>
<p>Note we use toInt to cast the second argument to an integer. This provides a big reduction in memory, as well as enabling numeric specific actions. </p>
<p>Another transformation that's very important, and is puported to be a buliding block of the map reduce paradigm is...map !</p>
<p>The map takes one line or element at a time, and transforms it to another. Let's take the following example : let's determine the maximum number of viewer that every channel has reached during the study.</p>
<p>First, we map each line in order to have couples of the form (<strong>The channel name, number of viewers</strong>). Note that a channel will have several of these lines with different number of viewers depending on the recording time.</p>
<pre><code>val audience = freads.map(x =&gt; (x.split("\t")(3), x.split("\t")(1).toInt))</code></pre>
<p>As the second part of the list is an integer we can use the stats() action for Spark to compute some information about the split in channel popularity across the dataset. In order to select the second element of a tuple, we map the tuple to its ._2 part (._1 would select the first one, so it is not zero indexed).</p>
<pre><code>audience.map(x=&gt;x._2).stats()</code></pre>
<p>Try taking a relatively large sample of audience, and notice that many of the channels have the name null or empty string, these lines are misleading as they represent corrupted data. Try filtering</p>
<pre><code>val audienceFiltered = audience.filter(x =&gt; (x._1!="" &amp;&amp; x._1!="null"))</code></pre>
<p></p>
<h3>Grouping data together</h3>
<p>The main value of Spark parallel computations (and its original Map/Reduce implementation) is to go beyond embarrasingly parallel operations such as map or filter, and be able to automatically parallelise many to many communications patterns. The groupBy operation of Spark provides the baseline method, it takes a list of pairs, and puts together all the values belonging to each key, so that they can be consequently reduced. A common pattern is to reduce all the values from each list into a single one, in order to compute some aggregate results.</p>
<p>We are going to use this pattern for computing some data related to each channel. We are going to obtain the peak audience.</p>
<pre><code>val maxs = audienceFiltered.reduceByKey((x,y) =&gt; math.max(x,y))</code></pre>
<p>Again, we can use an action such as maxs.stats() to force program execution and retrieve these values.</p>
<p><strong>Advanced Excercise :</strong></p>
<p>By using these functions, try to write the following programs, and answer the following questions.</p>
<ul>
<li>Get the viewing figures per game. What is the most popular game on average? You can reduce averages by using <em>RDD.aggregateByKey(initialization),function_one, function_two)</em>. We will use an accumulator which is two values, (sum, number_of_elements). The sum is the sum of the viewers over all periods so far, and the number of elements is how many elements have with the same key have we summed so far. An aggregationByKey needs an initialization of the <em>accumulator</em>, and two function :
<ul>
<li>The first is to aggregate between (aggregated_value, intermediate_aggr) + (value)</li>
<li>The second to aggregate couples of (aggregated_value1, intermediate_aggr1) + (aggregated_value2, intermediate_aggr2). Try looking for the aggregator on the official Spark Documentation to get used to this reflexe.</li>
<li>Finally map each entry to divide the number of viewers by the number of timestamps. You can use map as we did before, or use mapValues, The difference is that the new values you obtain from the aggragation are couples (ie. (key,(sum,n)). And we need to divide sum by n, so it would be easier to use mapValues which is easier.</li>
<li>RDDs are not sorted, so in order to retrieve the highest value you will need to sort <br />the resulting RDD according to their value (remember, channel is the key, and the average is the value).
<p></p>
</li>
</ul>
</li>
</ul>
<p>If you find it difficult to find out how to do it, The code for that is the following  :</p>
<pre>audience.aggregateByKey((<span class="pl-c1">0</span>,<span class="pl-c1">0</span>))(<br />        (acc,value) <span class="pl-k">=&gt;</span> (acc._1 <span class="pl-k">+</span> value, acc._2 <span class="pl-k">+</span> <span class="pl-c1">1</span>),
        (acc1,acc2) <span class="pl-k">=&gt;</span> (acc1._1 <span class="pl-k">+</span> acc2._1, acc1._2 <span class="pl-k">+</span> acc2._2))<br />        .mapValues(sumCount <span class="pl-k">=&gt;</span> <span class="pl-c1">1.0</span> <span class="pl-k">*</span> sumCount._1 <span class="pl-k">/</span> sumCount._2)<code><br /></code><br />        .sortBy(_._2, false)<br />        .take(10)</pre>
<ul>
<li>What is the day with the highest number of registered views from the dataset? Now you will need to select the day (can be extracted from the date by doing...), and group the data by day rather than the specific game.</li>
</ul>
<h3>Caching and saving the results</h3>
<p>You might have noticed how Spark recreates the whole chain of transformations whenever we invoke a new action. In order to enable faster results, and provide more interactive interactions through the command shell, we can take advantage of Spark's caching functionality to make an RDD persistent in memory. We do that by invoking the persist transformation.</p>
<pre>val inmem = maxs.persist()</pre>
<p>Persisted RDDs are kept in memory after executing the first time. This means that multiple transformations and actions over them will be executed substantially faster, as there is no need to recreate them from HDFS, as well as previous transformation steps. We will take advantage of that to interact with it, checking what is the count for several words. </p>
<p>We will use the filter transformation (see Spark documentation) to select only one element from the results. As we want to get the count for a specific word, we need to select the item from the RDD whose key is the word we are looking for. We can select the first element of a Scala tuple with the expression tuple._1 as shown in the code below.</p>
<p>Additionally, we need to invoke an action after our transformation, as otherwise the single value will be still distributed in the cluster. By invoking collect on the filtered RDD we will get back the result we are looking for to our driver program. The following Spark instruction will therefore return the value we are looking for:</p>
<pre>inmem.filter(pair=&gt;pair._1.equals("\"LeagueOfLegends\"")).collect()</pre>
Note that the quotes are part of the string so they need to be included and escaped.
<ul>
<li>What is the peak audience for the game LeagueOfLegends in a single channel?  Modify now this last query (you can push the up arrow in the keyboard to reload the last command for quickly editing), and check the popularity of other games streamed through a single channel (e.g. "Hearthstone"). You should see substantially improved performance for all these subsequent invocations. This is thanks to the persist() transformation we invoked early. You can  </li>
</ul>
<p>Finally, you can save any RDD back to HDFS by invoking the saveAsTextFile action (if the desired output format is TextOutputFormat). You have to specify the absolute path inside hdfs for that, for which you need to know what is your user account in HDFS.</p>
<pre>inmem.saveAsTextFile("hdfs://moonshot-ha-nameservice/user/&lt;&lt;enterYourUserAccount&gt;&gt;/CISGraphL1")</pre>
<h3></h3>
<h3>Joining multiple datasets</h3>
<p>In a real dataset it is common to want to combine multiple datasets in order to merge different information sources. This is commonly implemented  by join functions in a database. Spark also provides a set of join transformations, whihch allow to combine the values from two different RDDs, by matching their keys. The syntax is rdd1.join(rdd2). The only requirement is that both datasets have as the first tuple element (aka key), the same elements.</p>
<p>We will load a second dataset from HDFS which contains the genres for each game that appears in the popularity traces.</p>
<p>The file of game genres is in the same folder, first load the file into an RDD. Then let's make sure to make a tuple (primary key, value) where primary key is the name of the game, and value is the genre.</p>
<pre>val genres= sc.textFile("hdfs://moonshot-ha-nameservice/camp/game.genre.txt").<br />   filter(x =&gt; x.split("\t").length &gt;1).<br />   map(x =&gt; (x.split("\t")(0),x.split("\t")(1)))</pre>
<p>Next, we do the same thing with the channels dataset, we retrieve the game name as a primary key in an RDD, then we plug the rest of the line data into the value.</p>
<pre>val channels = audienceFiltered.map(x =&gt; (<code>x._1</code>.substring(1,x._1.length-1), <code>x._2</code>))</pre>
<p>We load the dataset as an RDD Tuple, with the first element being the game name, and the second being the genre name. </p>
<p>We can now use join to compute information aggregated by genre, rather than title, by joining both datasets as follows.</p>
<p>First, we need to transform the popularity dataset into a tuple where the first parameter is the game, followed by the rest.</p>
<pre>val joinedRDD = channels.join(genres)<br />joinedRDD.take(10).foreach(println)</pre>
<h3></h3>
<h3>BONUS 1: Data mining in Spark: Using Machine Learning algorithms</h3>
<p>Spark incorporates a project with several statistics and machine learning methods implemented that simplifies large-scale data mining, called ML. You can learn more about its functionality in the official Spark documentation. Do keep in mind that the same performance limitaitons that appear when implementing base Spark workflows will appear when using these tools, but they might be hidden by the opacity of the library implementation. </p>
<p>You will need to import beforehand the package in the command line.</p>
<p></p>
<p><a href="http://spark.apache.org/docs/latest/mllib-statistics.html" class="_blanktarget">http://spark.apache.org/docs/latest/mllib-statistics.html</a></p>
<p><a href="http://spark.apache.org/docs/latest/ml-guide.html" class="_blanktarget">http://spark.apache.org/docs/latest/ml-guide.html</a></p>
<p></p>
<h3>BONUS 2: Creating Spark projects </h3>
</section>
