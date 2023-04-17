# KafkaStream
DADS6005 Kafka Stream


<h2>Instructions</h2>
<p>Follow the instructions below to run the project:</p>
<ol>
	<li>Clone the repository to your local machine.</li>
	<li>Install Apache Kafka on your machine.</li>
	<li>Start the Kafka Connect and Kafka Streams services.</li>
	<li>Configure the Kafka Connect source connector to read the Harry Potter novel file and the sink connector to write the output to a file or a visualization tool.</li>
	<li>Run the Kafka Streams application to analyze the data and generate real-time analytics.</li>
</ol>

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
	<li>D3.js</li>
</ul>

<h2>Contributors</h2>
<ul>
	<li>John Doe</li>
	<li>Jane Smith</li>
</ul>




docker compose up -d
curl -d @"source.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors
curl -d @"source-sink.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors

./gradlew build



./gradlew runStreams -Pargs=basic


