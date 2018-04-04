import time
start_time=time.time();
from cassandra.cluster import Cluster
import os
import json
import re
cluster = Cluster()
session = cluster.connect()
session.execute("DROP KEYSPACE IF EXISTS test;")
session.execute("CREATE KEYSPACE test WITH replication ={'class':'SimpleStrategy','replication_factor':'1'} AND durable_writes=true;")
session.execute('USE test;')
session.execute('CREATE TABLE tweets(pid BIGINT PRIMARY KEY, quote_count INT , reply_count INT, hashtags LIST<VARCHAR>,date_time TIMESTAMP, date_ VARCHAR,like_count INT,verified BOOLEAN,sentiment INT,author VARCHAR,location VARCHAR,tid BIGINT,retweet_count INT,type VARCHAR,media_list LIST<VARCHAR>,quoted_source_id varchar,url_list LIST<VARCHAR>,tweet_text varchar,author_profile_image varchar,author_screen_name varchar,author_id int,lang varchar,keywords_processed_list LIST<VARCHAR>,retweet_source_id int,mentions LIST <VARCHAR>,replyto_source_id BIGINT);')

current_directory=os.getcwd();
number_of_files=0;



current_file_number=0;	
for x in os.listdir('./workshop_dataset1'):
	current_file_number+=1;
	print("progress is = ",current_file_number,"/",113,"time=",time.time()-start_time)
	
	x='./workshop_dataset1/'+x;
	fp=open(x,'r');
	text=json.load(fp);
	attributes=["quote_count","reply_count","hashtags","datetime","date","like_count","verified","sentiment","author","location","tid","retweet_count","type","media_list","quoted_source_id","url_list","tweet_text","author_profile_image","author_screen_name","author_id","lang","keywords_processed_list","retweet_source_id","mentions","replyto_source_id"]
	for i in text:
		for j in range(len(attributes)):
			print(text[i][attributes[j]],end=" ")
		print()
		print()
		
		# print(text[i]['quote_count'])


	exit()
	# print(len(text));
	# exit();
	# text=fp.read();
	# left_curly=-1;
	# data=[];
	# # text=text.split("\"quote_count\"");
	# text=text.splitlines();
	# text="".join(text)
	# text=text.split("\"quote_count\":")

	# for row_number in range(len(text)):
	# 	text[row_number]=re.split('"quote_count":|"reply_count":|"hashtags":|"datetime":|"date":|"like_count":|"verified":|"sentiment":|"author":|"location":|"tid":|"retweet_count":|"type":|"media_list":|"quoted_source_id":|"url_list":|"tweet_text":|"author_profile_image":|"author_screen_name":|"author_id":|"lang":|"keywords_processed_list":|"retweet_source_id":|"mentions":|"replyto_source_id":',text[row_number]);
	
	# 	if(0):
	# 		print(x);
	# 		print(len(text))
	# 		# print(text)
	# 		print()
	# 		print()
	# 		print()
	# 	else:
	# 		if((len(text[row_number])-1)%24):
	# 			print(x);
	# 			print(len(text[row_number]))

	# for row_number in range(1,len(text)):
	# 	row=text[row_number];
	# 	for i in range(len(row)):
	# 		row[i]=row[i].lstrip().rstrip()[0:-1];
	# 		if(i==2 or i==13 or i==15 or i==21 or i==23):
	# 			if(row[i][0]=='[' or row[i][0]=='{'):
	# 				row[i]=row[i][1:-1].lstrip().rstrip();
	# 		if(i==2 or i==15):
	# 			row[i]=row[i].split(',')
	# 			for sub_row_num in range(len(row[i])):
	# 				row[i][sub_row_num]=row[i][sub_row_num].lstrip().rstrip();
	# 		if(i==21):
	# 			row[i]=row[i].split('",  ')
	# 			for sub_row_num in range(len(row[i])):
	# 				row[i][sub_row_num]=row[i][sub_row_num].lstrip().rstrip();
	# 				print(row[i][sub_row_num][0])
				# print(row[i])

				# print(row[i].split("             "))

		# print(row)
		# print();
		# print()

	# exit();
	
	number_of_files+=1;


print(number_of_files, " are the number of files");

# session.execute("INSERT INTO tweets (pid)VALUES(934934507945312256);")
# rows=session.execute('SELECT * FROM tweets;')
# for user_row in rows:
# 	print(user_row);
print("time=",time.time()-start_time);