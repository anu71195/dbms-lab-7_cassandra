import time,os,json
start_time=time.time();
from cassandra.cluster import Cluster

session = Cluster().connect();
session.execute("DROP KEYSPACE IF EXISTS twitter;")
session.execute("CREATE KEYSPACE twitter WITH replication ={'class':'SimpleStrategy','replication_factor':'1'} AND durable_writes=true;")
session.execute('USE twitter;')
session.execute('CREATE TABLE tweets(tid BIGINT PRIMARY KEY, quote_count INT , reply_count INT, hashtags VARCHAR,date_time TIMESTAMP, date_ date,like_count INT,verified BOOLEAN,sentiment INT,author VARCHAR,location VARCHAR,retweet_count INT,type VARCHAR,media_list VARCHAR,quoted_source_id BIGINT,url_list VARCHAR,tweet_text varchar,author_profile_image varchar,author_screen_name varchar,author_id BIGINT,lang varchar,keywords_processed_list VARCHAR,retweet_source_id BIGINT,mentions VARCHAR,replyto_source_id BIGINT);')
session.execute('CREATE TABLE tweets_1(tid BIGINT ,date_time TIMESTAMP, date_ date,author VARCHAR,location VARCHAR,tweet_text varchar,author_screen_name varchar,author_id BIGINT,lang varchar, primary key(author, date_time,tid));')
session.execute('CREATE TABLE tweets_2(tid BIGINT, keywords_processed_list VARCHAR , like_count INT , PRIMARY KEY(keywords_processed_list,like_count,tid))')
session.execute('CREATE TABLE tweets_3(tid BIGINT, hashtag VARCHAR,date_time TIMESTAMP, date_ date,PRIMARY KEY(hashtag,date_time,tid))')
session.execute('CREATE TABLE tweets_4(tid BIGINT, author varchar, mentions VARCHAR , date_time TIMESTAMP, date_ date, PRIMARY KEY(author,date_time,tid))')
session.execute('CREATE TABLE tweets_5(tid BIGINT, date_time TIMESTAMP, date_ date, like_count INT,primary key(date_,like_count,tid))')
session.execute('CREATE TABLE tweets_6(tid BIGINT, location VARCHAR, tweet_text varchar, primary key(location,tid))')
session.execute('CREATE TABLE tweets_7(tid BIGINT, date_time TIMESTAMP, date_ date, hashtags varchar, primary key(date_,tid))')

def get_query(query,x,attributes,prime,check):
	x='./workshop_dataset1/'+x
	fp=open(x,'r');
	text=json.load(fp);
	fp.close();
	for i in text:
		if(check):
			if(text[i][prime]==None):
				return;
			if(type(text[i][prime])==type([])):
				flag=0;
				for list_len in range(len(text[i][prime])):
					if(text[i][prime][list_len]!=""):
						flag=1;
					if(flag==0):
						return;

		queries=[query+str(text[i][attributes[0]])]
		for j in range(1,len(attributes)):
			if(text[i][attributes[j]]==None):
				for no_queries in range(len(queries)):
					queries[no_queries]=queries[no_queries]+","+"null"
			elif(attributes[j]=="datetime" or attributes[j]=="media_list" or attributes[j]=="date" or attributes[j]=="author" or attributes[j]=="location" or attributes[j]=="type" or attributes[j]=="tweet_text" or attributes[j]=="author_profile_image" or attributes[j]=="author_screen_name" or attributes[j]=="lang"):
				for no_queries in range(len(queries)):	
					queries[no_queries]=queries[no_queries]+",'"+(str(text[i][attributes[j]])).replace("'","''")+"'";
			elif (attributes[j]=="hashtags" or attributes[j]=="url_list" or attributes[j]=="keywords_processed_list" or attributes[j]=="mentions"):
				no_of_queries=len(queries);
				for no_queries in range(no_of_queries):
					temp_query=queries[no_queries];
					queries[no_queries]+=",'"+(str(text[i][attributes[j]][0])).replace("'","''")+"'";
					for k in range(1,len(text[i][attributes[j]])):
						queries.append(temp_query+",'"+(str(text[i][attributes[j]][k])).replace("'","''")+"'");
			else:
				for no_queries in range(len(queries)):
					queries[no_queries]=queries[no_queries]+","+(str(text[i][attributes[j]]));
		
		for no_queries in range(len(queries)):
			queries[no_queries]+=");"
			session.execute(queries[no_queries])
	

print(time.time()-start_time)
current_directory=os.getcwd();
current_file_number=0;	

attributes=["quote_count","reply_count","hashtags","datetime","date","like_count","verified","sentiment","author","location","tid","retweet_count","type","media_list","quoted_source_id","url_list","tweet_text","author_profile_image","author_screen_name","author_id","lang","keywords_processed_list","retweet_source_id","mentions","replyto_source_id"]
attributes1=["tid","datetime","date","author","location","tweet_text","author_screen_name","author_id","lang"]
attributes2=["tid","keywords_processed_list","like_count"]
attributes3=["tid","hashtags","date","datetime"]
attributes4=["tid","author","mentions","datetime","date"]
attributes5=["tid","datetime","date","like_count"]
attributes6=["tid","location","tweet_text"]
attributes7=["tid","datetime","date","hashtags"]

query_template="INSERT INTO tweets(quote_count,reply_count,hashtags,date_time,date_,like_count,verified,sentiment,author,location,tid,retweet_count,type,media_list,quoted_source_id,url_list,tweet_text,author_profile_image,author_screen_name,author_id,lang,keywords_processed_list,retweet_source_id,mentions,replyto_source_id) VALUES(";
query1_template="INSERT INTO tweets_1(tid,date_time,date_,author,location,tweet_text,author_screen_name,author_id,lang) VALUES("
query2_template="INSERT INTO tweets_2(tid,keywords_processed_list,like_count) VALUES("
query3_template="INSERT INTO tweets_3(tid,hashtag,date_,date_time) VALUES("
query4_template="INSERT INTO tweets_4(tid,author,mentions,date_time,date_) VALUES("
query5_template="INSERT INTO tweets_5(tid,date_time,date_,like_count) VALUES("
query6_template="INSERT INTO tweets_6(tid,location,tweet_text) VALUES("
query7_template="INSERT INTO tweets_7(tid,date_time,date_,hashtags) VALUES("

for x in os.listdir('./workshop_dataset1'):
	current_file_number+=1;
	print("progress is = ",current_file_number,"/",113,"time=",time.time()-start_time)	
	# query=get_query(query_template,text,i,attributes)
	query1=get_query(query1_template,x,attributes1,"na",0)
	query2=get_query(query2_template,x,attributes2,"keywords_processed_list",1)
	query3=get_query(query3_template,x,attributes3,"hashtags",1)
	query4=get_query(query4_template,x,attributes4,"author",1)
	query5=get_query(query5_template,x,attributes5,"na",0)
	query6=get_query(query6_template,x,attributes6,"location",1)
	query7=get_query(query7_template,x,attributes7,"na",0)
#session.execute('ALTER TABLE tweets WITH GC_GRACE_SECONDS = 0;');

rows=session.execute('SELECT * FROM tweets;')
for user_row in rows:
	print(user_row);
print("time=",time.time()-start_time);