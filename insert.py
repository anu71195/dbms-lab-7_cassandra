import time,os,json
start_time=time.time();
from cassandra.cluster import Cluster
session = Cluster().connect();
session.execute("DROP KEYSPACE IF EXISTS twitter;")
session.execute("CREATE KEYSPACE twitter WITH replication ={'class':'SimpleStrategy','replication_factor':'1'} AND durable_writes=true;")
session.execute('USE twitter;')
session.execute('CREATE TABLE tweets(tid BIGINT PRIMARY KEY, quote_count INT , reply_count INT, hashtags LIST<VARCHAR>,date_time TIMESTAMP, date_ VARCHAR,like_count INT,verified BOOLEAN,sentiment INT,author VARCHAR,location VARCHAR,retweet_count INT,type VARCHAR,media_list VARCHAR,quoted_source_id BIGINT,url_list LIST<VARCHAR>,tweet_text varchar,author_profile_image varchar,author_screen_name varchar,author_id BIGINT,lang varchar,keywords_processed_list LIST<VARCHAR>,retweet_source_id BIGINT,mentions LIST <VARCHAR>,replyto_source_id BIGINT);')
print(time.time()-start_time)
current_directory=os.getcwd();
current_file_number=0;	
for x in os.listdir('./workshop_dataset1'):
	current_file_number+=1;
	print("progress is = ",current_file_number,"/",113,"time=",time.time()-start_time)	
	x='./workshop_dataset1/'+x;
	fp=open(x,'r');
	text=json.load(fp);
	attributes=["quote_count","reply_count","hashtags","datetime","date","like_count","verified","sentiment","author","location","tid","retweet_count","type","media_list","quoted_source_id","url_list","tweet_text","author_profile_image","author_screen_name","author_id","lang","keywords_processed_list","retweet_source_id","mentions","replyto_source_id"]
	for i in text:
		query="INSERT INTO tweets(quote_count,reply_count,hashtags,date_time,date_,like_count,verified,sentiment,author,location,tid,retweet_count,type,media_list,quoted_source_id,url_list,tweet_text,author_profile_image,author_screen_name,author_id,lang,keywords_processed_list,retweet_source_id,mentions,replyto_source_id) VALUES(";
		query=query+str(text[i][attributes[0]])
		for j in range(1,len(attributes)):
			if(text[i][attributes[j]]==None):
				query=query+","+"null"
			elif(attributes[j]=="datetime" or attributes[j]=="media_list" or attributes[j]=="date" or attributes[j]=="author" or attributes[j]=="location" or attributes[j]=="type" or attributes[j]=="tweet_text" or attributes[j]=="author_profile_image" or attributes[j]=="author_screen_name" or attributes[j]=="lang"):
				query=query+",'"+(str(text[i][attributes[j]])).replace("'","''")+"'";
			elif (attributes[j]=="hashtags" or attributes[j]=="url_list" or attributes[j]=="keywords_processed_list" or attributes[j]=="mentions"):
				temp="["
				for k in range(len(text[i][attributes[j]])):
					text[i][attributes[j]][k]=text[i][attributes[j]][k].replace("'","''")
					temp+="'"+text[i][attributes[j]][k]+"'"+','
				query=query+","+temp[0:-1]+']';
			else:
				query=query+","+(str(text[i][attributes[j]]));			
		query=query+");"
		# print(query+"\n")
		session.execute(query)
rows=session.execute('SELECT * FROM tweets;')
for user_row in rows:
	print(user_row);
print("time=",time.time()-start_time);