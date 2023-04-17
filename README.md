# KafkaStream
DADS6005 Kafka Stream

<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8">
    <title>Harry Potter Real-Time Analytics</title>
  </head>
  <body>
    <h1>Instructions</h1>
    <p>Design (draw a diagram showing the components) and develop a real-time analysis system for the Harry Potter novel (txt file).</p>
    <p>Use Kafka connect to connect the Harry file (source) <code>book.txt</code> and the output file (sink). You can use graph (bar charts) or text mode to display the results as desired.</p>
    <p>Use Kafka streams (Java) to analyze the data as follows:</p>
    <ol>
      <li>Sink-side displays the results of word count in each chapter (excluding stop words).</li>
      <li>Sink-side displays the results of sentence count containing the word "potter" in each chapter (including stop words in the sentence).</li>
    </ol>
   
</html>


 

<h2>Features</h2>
<p>The project provides the following features:</p>
<ul>
	<li>Kafka Connect source connector to read the Harry Potter novel file.</li>
	<li>Kafka Connect sink connector to write the output to a file or a visualization tool.</li>
	<li>Kafka Streams application to analyze the data and generate real-time analytics.</li>
	<li>Real-time analytics for word counts in each chapter, excluding stop words.</li>
	<li>Real-time analytics for sentence counts containing the word "Potter" in each chapter, including stop words.</li>
</ul>

<h2>Technologies Used</h2>
<ul>
	<li>Apache Kafka</li>
	<li>Kafka Connect</li>
	<li>Kafka Streams</li>
	<li>Java</li>
	<li>HTML</li>
	<li>CSS</li>
	<li>JavaScript</li>

</ul>



<h1>Harry Potter Real-Time Analytics</h1>
<h2>Instructions</h2>
<p>To use the system, follow these instructions:</p>
<ol>
<li>In a terminal, navigate to the root directory of the project.</li>
<li>Run the following command to start the Kafka Connect and Kafka Streams services:</li>
<pre><code>docker-compose up -d</code></pre>
<li>Run the following commands to create the Kafka Connect connectors:</li>
<pre><code>curl -d @"source.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors
curl -d @"source-sink1.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors</code></pre>
<pre><code>curl -d @"source.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors
curl -d @"source-sink2.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors</code></pre>
<li>Build the project using the following command:</li>
<pre><code>./gradlew build</code></pre>
<li>Run the Kafka Streams application using the following command:</li>
<pre><code>./gradlew runStreams -Pargs=basic</code></pre>
</ol>
<h2>Contributors</h2>
<ul>
	<li>Witsarut Wongsim</li>
	<li>สุชาวลี จีระธัญญาสกุล</li>
	<Li>ธนากร วิธุรัติ</Li>
</ul>

