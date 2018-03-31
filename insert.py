# from cassandra.cluster import Cluster
import os
# cluster = Cluster()
# session = cluster.connect()
# session.execute("DROP KEYSPACE IF EXISTS test;")
# session.execute("CREATE KEYSPACE test WITH replication ={'class':'SimpleStrategy','replication_factor':'1'} AND durable_writes=true;")
# session.execute('USE test;')
# session.execute('CREATE TABLE tweets(pid BIGINT PRIMARY KEY, quote_count INT , reply_count INT, hashtags LIST<VARCHAR>,date_time TIMESTAMP, date_ VARCHAR,like_count INT,verified BOOLEAN,sentiment INT,author VARCHAR,location VARCHAR,tid BIGINT,retweet_count INT,type VARCHAR,media_list LIST<VARCHAR>,quoted_source_id varchar,url_list LIST<VARCHAR>,tweet_text varchar,author_profile_image varchar,author_screen_name varchar,author_id int,lang varchar,keywords_processed_list LIST<VARCHAR>,retweet_source_id int,mentions LIST <VARCHAR>,replyto_source_id BIGINT);')

current_directory=os.getcwd();
number_of_files=0;

def get_num_attribute_value(i,text):
	start_index=i;
	while(text[i]!=','):
		i+=1;
	end_index=i
	quote_count_text=text[start_index:end_index]
	quote_count_text=quote_count_text.split(":")
	quote_count_text[1]=int(quote_count_text[1])
	return quote_count_text,i;

def get_list():
	
for x in os.listdir('./workshop_dataset1'):
	print(x);
	x='./workshop_dataset1/'+x;
	fp=open(x,'r');
	text=fp.read();
	left_curly=0;
	for i in range(len(text)):
		if(text[i]=='{'):
			left_curly+=1;
		elif (text[i]=='}'):
			left_curly-=1;
		if(text[i:i+14]=="\"quote_count\":"):
			quote_count_text,i=get_attribute_value(i,text)
			print(quote_count_text)
		elif(text[i:i+14]=="\"reply_count\":"):
			reply_count_text,i=get_num_attribute_value(i,text)
			print(reply_count_text)
		elif(text[i:i+11]=="\"hashtags\":"):
			hash_list=get_list();
			print(hash_list)
	number_of_files+=1;


print(number_of_files, " are the number of files");

# session.execute("INSERT INTO tweets (pid)VALUES(934934507945312256);")
# rows=session.execute('SELECT * FROM tweets;')
# for user_row in rows:
# 	print(user_row);