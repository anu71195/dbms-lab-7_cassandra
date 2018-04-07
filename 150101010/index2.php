<?php  
	error_reporting(E_ALL);
	ini_set('display_errors', '1');
	$cluster = Cassandra::cluster()
				->build();
	$keyspace = "twitter";
	$session = $cluster->connect($keyspace);
?>
<!DOCTYPE html>
<html>
<head>
	<title>CS345 Cassandra</title>
</head>
<body>
<form method="post" action="index2.php">
	<input type="text" name="query">
	<input type="submit" name="submit">
	<br>
	<br>
	<?php
		if(!empty($_POST))
		{
			$statement = new Cassandra\SimpleStatement($_POST["query"]);
			$future = $session->executeAsync($statement);
			$result = $future->get();
			foreach ($result as $rows) {
				foreach ($rows as $vals) {
					echo $vals."<br>";
				}
				echo "<hr>";
			}
		}
		else{
			echo "Enter a query";
		}
	?>
</form>
</body>
</html>
