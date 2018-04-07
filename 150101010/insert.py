import time,os,json
start_time=time.time();
from cassandra.cluster import Cluster
cluster=Cluster();
session = cluster.connect();
session.execute("DROP KEYSPACE IF EXISTS twitter;")
session.execute("CREATE KEYSPACE twitter WITH replication ={'class':'SimpleStrategy','replication_factor':'1'} AND durable_writes=true;")
session.execute('USE twitter;')
# session.execute('CREATE TABLE tweets(tid BIGINT PRIMARY KEY, quote_count INT , reply_count INT, hashtags VARCHAR,date_time TIMESTAMP, date_ date,like_count INT,verified BOOLEAN,sentiment INT,author VARCHAR,location VARCHAR,retweet_count INT,type VARCHAR,media_list VARCHAR,quoted_source_id BIGINT,url_list VARCHAR,tweet_text varchar,author_profile_image varchar,author_screen_name varchar,author_id BIGINT,lang varchar,keywords_processed_list VARCHAR,retweet_source_id BIGINT,mentions VARCHAR,replyto_source_id BIGINT);')
# session.execute('CREATE TABLE tweets_1(tid BIGINT ,date_time TIMESTAMP, date_ date,author VARCHAR,location VARCHAR,tweet_text varchar,author_screen_name varchar,author_id BIGINT,lang varchar, primary key(author, date_time,tid));')
# session.execute('CREATE TABLE tweets_2(tid BIGINT,tweet_text varchar,author_id BIGINT,location varchar,lang varchar, keywords_processed_list VARCHAR , like_count INT , PRIMARY KEY(keywords_processed_list,like_count,tid))')
# session.execute('CREATE TABLE tweets_3(tid BIGINT, tweet_text varchar,author_id BIGINT,location varchar,lang varchar,hashtag varchar,date_ date,date_time timestamp,PRIMARY KEY(hashtag,date_time,tid))')
# session.execute('CREATE TABLE tweets_4(tid BIGINT, tweet_text varchar, author varchar, author_id bigint,location varchar, lang varchar,mentions VARCHAR , date_time TIMESTAMP, date_ date, PRIMARY KEY(author,date_time,tid))')
# session.execute('CREATE TABLE tweets_5(tid BIGINT, tweet_text varchar, author_id bigint, location varchar, lang varchar, date_time TIMESTAMP, date_ date, like_count INT,primary key(date_,like_count,tid))')
# session.execute('CREATE TABLE tweets_6(tid BIGINT, tweet_text varchar, author_id bigint, location VARCHAR, lang varchar, primary key(location,tid))')
# session.execute('CREATE TABLE tweets_7(frequency INT, date_ date, hashtags varchar, primary key(date_,frequency,hashtags))')
session.execute('CREATE TABLE tweets_8(frequency INT, location varchar, mentions varchar, primary key(location,frequency,mentions));')
session.execute('CREATE TABLE tweets_9(date_ date, frequency int,location varchar, primary key(date_,frequency,location));')


def get_query(query,text,attributes,prime,check):
	for i in text:
		# print(i)
		# print(text[i]['keywords_processed_list'])
		if(check):
			if(text[i][prime]==None):
				continue;
			# print(text[i]['keywords_processed_list'])
			if(type(text[i][prime])==type([])):
				flag=0;
				for list_len in range(len(text[i][prime])):
					if(text[i][prime][list_len]!=""):
						flag=1;
					# print("a",text[i][prime][list_len]=="")
					# print("a",text[i][prime][list_len]=='')

				if(flag==0):
					continue;

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
						# print(text[i][attributes[j]][k])
			else:
				for no_queries in range(len(queries)):
					queries[no_queries]=queries[no_queries]+","+(str(text[i][attributes[j]]));
		
		for no_queries in range(len(queries)):
			queries[no_queries]+=");"
			session.execute(queries[no_queries])
			
			# print(queries[no_queries])
			# print()


def get_query_2(query,text,attributes,prime,check):
	dictionary={};
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
		if(type(text[i]['hashtags'])==type([])):
			for j in range(len(text[i]['hashtags'])):
				dictionary[text[i]['hashtags'][j]]=0;

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
				if(attributes[j]=="hashtags"):
					for no_queries in range(no_of_queries):
						temp_query=queries[no_queries];
						queries[no_queries]+=",'"+(str(text[i][attributes[j]][0])).replace("'","''")+"'";
						dictionary[str(text[i][attributes[j]][0])]+=1;
						for k in range(1,len(text[i][attributes[j]])):
							queries.append(temp_query+",'"+(str(text[i][attributes[j]][k])).replace("'","''")+"'");
							dictionary[str(text[i][attributes[j]][k])]+=1;
				else:
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
			# session.execute(queries[no_queries])
	return dictionary;
def get_query_3(query,text,attributes,prime,check,dictionary):
	for i in text:
		if(check):
			if(text[i][prime]==None ):
				continue;
			if(type(text[i][prime])==type([])):
				flag=0;
				for list_len in range(len(text[i][prime])):
					if(text[i][prime][list_len]!=""):
						flag=1;
					if(flag==0):
						continue;
		dictionary[text[i]['location']]={};

		if(type(text[i]['mentions'])==type([])):
			for j in range(len(text[i]['mentions'])):
				if(text[i]['mentions'][j] not in dictionary[text[i]['location']] ):
					dictionary[text[i]['location']][text[i]['mentions'][j]]=0;

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
				if(attributes[j]=="mentions"):
					for no_queries in range(no_of_queries):
						temp_query=queries[no_queries];
						queries[no_queries]+=",'"+(str(text[i][attributes[j]][0])).replace("'","''")+"'";
						dictionary[text[i]['location']][str(text[i][attributes[j]][0])]+=1;
						for k in range(1,len(text[i][attributes[j]])):
							queries.append(temp_query+",'"+(str(text[i][attributes[j]][k])).replace("'","''")+"'");
							dictionary[text[i]['location']][str(text[i][attributes[j]][k])]+=1;
				else:
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
			# session.execute(queries[no_queries])
	return dictionary;

def get_query_4(query,text,attributes,prime,check,prime2,dictionary):
	for i in text:
		if(check):
			if(text[i][prime]==None or text[i][prime2]==None):
				continue;
			if(type(text[i][prime])==type([])):
				flag=0;
				for list_len in range(len(text[i][prime])):
					if(text[i][prime][list_len]!=""):
						flag=1;

				if(flag==0):
					continue;
			if(type(text[i][prime2])==type([])):
				flag=0;
				for list_len in range(len(text[i][prime2])):
					if(text[i][prime2][list_len]!=""):
						flag=1;

				if(flag==0):
					continue;
		for men in text[i]["mentions"]:
			men_loc=men+text[i]["location"]
			# print(men_loc)
			if(men_loc not in dictionary):
				dictionary[men_loc]=1;
			else:
				dictionary[men_loc]+=1;
		

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
						# print(text[i][attributes[j]][k])
			else:
				for no_queries in range(len(queries)):
					queries[no_queries]=queries[no_queries]+","+(str(text[i][attributes[j]]));
		
		for no_queries in range(len(queries)):
			queries[no_queries]+=");"
			# session.execute(queries[no_queries])
		# exit();			
			# print(queries[no_queries])
			# print()
	return dictionary


print(time.time()-start_time)
current_directory=os.getcwd();
current_file_number=0;	

attributes=["quote_count","reply_count","hashtags","datetime","date","like_count","verified","sentiment","author","location","tid","retweet_count","type","media_list","quoted_source_id","url_list","tweet_text","author_profile_image","author_screen_name","author_id","lang","keywords_processed_list","retweet_source_id","mentions","replyto_source_id"]
attributes1=["tid","datetime","date","author","location","tweet_text","author_screen_name","author_id","lang"]
attributes2=["tid","tweet_text","author_id","location","lang","keywords_processed_list","like_count"]
attributes3=["tid","tweet_text","author_id","location","lang","hashtags","date","datetime"]
attributes4=["tid","tweet_text","author","author_id","location","lang","mentions","datetime","date"]
attributes5=["tid","tweet_text","author_id","location","lang","datetime","date","like_count"]
attributes6=["tid","tweet_text","author_id","location","lang"]
attributes7=["date","hashtags"]
attributes8=["location","mentions"]
attributes9=["date","location"]


query_template="INSERT INTO tweets(quote_count,reply_count,hashtags,date_time,date_,like_count,verified,sentiment,author,location,tid,retweet_count,type,media_list,quoted_source_id,url_list,tweet_text,author_profile_image,author_screen_name,author_id,lang,keywords_processed_list,retweet_source_id,mentions,replyto_source_id) VALUES(";
query1_template="INSERT INTO tweets_1(tid,date_time,date_,author,location,tweet_text,author_screen_name,author_id,lang) VALUES("
query2_template="INSERT INTO tweets_2(tid,tweet_text,author_id,location,lang,keywords_processed_list,like_count) VALUES("
query3_template="INSERT INTO tweets_3(tid,tweet_text,author_id,location,lang,hashtag,date_,date_time) VALUES("
query4_template="INSERT INTO tweets_4(tid,tweet_text,author,author_id,location,lang,mentions,date_time,date_) VALUES("
query5_template="INSERT INTO tweets_5(tid,tweet_text,author_id,location,lang,date_time,date_,like_count) VALUES("
query6_template="INSERT INTO tweets_6(tid,tweet_text,author_id,location,lang) VALUES("
query7_template="INSERT INTO tweets_7(date_,frequency,hashtags) VALUES("
query8_template="INSERT INTO tweets_8(location,frequency,mentions) VALUES("
query9_template="INSERT INTO tweets_9(location,date_,frequency) VALUES("


dictionary={}

for x in os.listdir('./workshop_dataset1'):
	x='./workshop_dataset1/'+x
	fp=open(x,'r');
	text=json.load(fp);
	fp.close();
	current_file_number+=1;
	print("progress is = ",current_file_number,"/",113,"time=",time.time()-start_time)	
	dictionary_2={}

	dictionary=get_query_3(query_template,text,attributes,"location",1,dictionary);
	dictionary_2=get_query_4(query_template,text,attributes,"location",1,"mentions",dictionary_2);
	for temp in text:
		cur_date=text[temp]["date"];
		break;
	print(dictionary_2)
	print(len(dictionary_2))
	for i in dictionary_2 :
		# print(dictionary_2[i])
		print(query9_template+"'"+i.replace("'","''")+"','"+cur_date+"',"+str(dictionary_2[i])+");")
		session.execute(query9_template+"'"+i.replace("'","''")+"','"+cur_date+"',"+str(dictionary_2[i])+");");
	# print(query8)

	print(len(dictionary))

# print(query8['New Delhi'])


i=0;
for key1 in dictionary:
	for key2 in dictionary[key1]:
		print(query7_template+"'"+key1+"',"+str(dictionary[key1][key2])+",'"+key2+"');")
		session.execute(query8_template+"'"+key1.replace("'","''")+"',"+str(dictionary[key1][key2])+",'"+key2+"');");
		i+=1;
print(i)

	# query=get_query(query_template,text,i,attributes)
	# get_query(query1_template,text,attributes1,"na",0)
	# get_query(query2_template,text,attributes2,"keywords_processed_list",1)
	# get_query(query3_template,text,attributes3,"hashtags",1)
	# get_query(query4_template,text,attributes4,"author",1)
	# get_query(query5_template,text,attributes5,"na",0)
	# get_query(query6_template,text,attributes6,"location",1)
# 	query7=get_query_2(query7_template,text,attributes7,"na",0)
# 	for i in text:
# 		dictionary[text[i]["date"]]=query7
# 		break;
# dates=[]
# dates_group=[]
# for i in sorted(dictionary):
# 	dates.append(i)
# for i in range(6):
# 	temp=[];
# 	for j in range(0,i+1):
# 		temp.append(dates[j]);
# 	dates_group.append(temp)

# for i in range(len(dates)-7):
# 	temp=[]
# 	for j in range (i,i+7):
# 		temp.append(dates[j]);
# 	dates_group.append(temp)

# my_dict={};
# for i in range(len(dates_group)):	
# 	temp={};
# 	for j in (dates_group[i]):
# 		for k in dictionary[j]:
# 			temp[k]=0;
# 	my_dict[dates_group[i][len(dates_group[i])-1]]=temp;
# for i in range(len(dates_group)):
# 	for j in (dates_group[i]):
# 		for k in dictionary[j]:
# 			my_dict[dates_group[i][len(dates_group[i])-1]][k]+=1;

# for i in range(len(dates_group)):
# 	print("progress is ",i,"/",113," time= ",time.time()-start_time);
# 	for j in (dates_group[i]):
# 		for k in dictionary[j]:
# 			temp=query7_template+"'"+str(dates_group[i][len(dates_group[i])-1])+"',"+str(my_dict[dates_group[i][len(dates_group[i])-1]][k])+",'"+str(k)+"');"
# 			session.execute(temp);





#session.execute('ALTER TABLE tweets WITH GC_GRACE_SECONDS = 0;');

# rows=session.execute('SELECT * FROM tweets;')
# for user_row in rows:
# 	print(user_row);
# print("time=",time.time()-start_time);