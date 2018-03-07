#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "hiredispool.h"
#include "log.h"
#include "hiredis/hiredis.h"

/* The following lines make up our testing "framework" :) */
static int tests = 0, fails = 0;
#define test(_s) { printf("#%02d ", ++tests); printf(_s); }
#define test_cond(_c) if(_c) printf("\033[0;32mPASSED\033[0;0m\n"); else {printf("\033[0;31mFAILED\033[0;0m\n"); fails++;}


/* -------------------------------------------*/
/**
 * @brief  选择redis一个数据库
 *
 * @param conn		已链接的数据库链接
 * @param db_no		redis数据库编号
 *
 * @returns
 *			-1 失败
 *			0  成功
 */
/* -------------------------------------------*/
int rop_selectdatabase(redisContext *conn, void **rep, unsigned int db_no) {
	int retn = 0;
	redisReply *reply = NULL;

	/* 选择一个数据库 */
	reply = (redisReply *) redisCommand(conn, "select %d", db_no);
	if (reply == NULL) {
		fprintf(stderr, "[-][GMS_REDIS]Select database %d error!\n", db_no);
		log_(L_INFO | L_CONS, "[-][GMS_REDIS]Select database %d error!%s\n",
				db_no, conn->errstr);
		retn = -1;
		goto END;
	}

	printf("[+][GMS_REDIS]Select database %d SUCCESS!\n", db_no);
	log_(L_INFO | L_CONS, "[+][GMS_REDIS]Select database %d SUCCESS!\n", db_no);

	END: *rep = reply;
	return retn;
}

/* -------------------------------------------*/
/**
 * @brief  判断key值是否存在
 *
 * @param conn		已经建立的链接
 * @param key		需要寻找的key值
 *
 * @returns
 *				-1 失败
 *				1 存在
 *				0 不存在
 */
/* -------------------------------------------*/
int rop_is_key_exist(redisContext *conn, void **rep, const char* key) {
	int retn = 0;

	redisReply *reply = NULL;

	reply = (redisReply *) redisCommand(conn, "EXISTS %s", key);
	//rop_test_reply_type(reply);
	if (reply->type != REDIS_REPLY_INTEGER) {
		fprintf(stderr, "[-][GMS_REDIS]is key exist get wrong type!\n");
		log_(L_INFO | L_CONS, "[-][GMS_REDIS]is key exist get wrong type! %s\n",
				conn->errstr);
		retn = -1;
		goto END;
	}

	if (reply->integer == 1) {
		retn = 1;
	} else {
		retn = 0;
	}
	log_(L_INFO | L_CONS, "[+][GMS_REDIS]is key exist = %d\n", retn);

	END: *rep = reply;
	return retn;
}

/* -------------------------------------------*/
/**
 * @brief			删除一个key
 *
 * @param conn		已经建立的链接
 * @param key
 *
 * @returns
 *				-1 失败
 *				0 成功
 */
/* -------------------------------------------*/
int rop_del_key(redisContext *conn, void **rep, const char *key) {
	int retn = 0;
	redisReply *reply = NULL;

	reply = (redisReply *) redisCommand(conn, "DEL %s", key);
	if (reply->type != REDIS_REPLY_INTEGER) {
		fprintf(stderr, "[-][GMS_REDIS] DEL key %s ERROR\n", key);
		log_(L_INFO | L_CONS, "[-][GMS_REDIS] DEL key %s ERROR %s\n", key,
				conn->errstr);
		retn = -1;
		goto END;
	}

	if (reply->integer > 0) {
		retn = 0;
		log_(L_INFO | L_CONS, "[+][GMS_REDIS] DEL key %s Succse\n", key);
	} else {
		retn = -1;
	}

	END: *rep = reply;
	return retn;
}

/* -------------------------------------------*/
/**
 * @brief  设置一个key的删除时间 ，系统到达一定时间
 *			将会自动删除该KEY
 *
 * @param conn				已经建立好的链接
 * @param time		存活时间，单位秒
 *
 * @returns
 *		0	SUCC
 *		-1  FAIL
 */
/* -------------------------------------------*/
int rop_set_key_lifecycle(redisContext *conn, void **rep, const char *key,
		int time) {
	int retn = 0;
	redisReply *reply = NULL;

	reply = (redisReply *) redisCommand(conn, "EXPIRE %s %d", key, time);
	if (reply->type != REDIS_REPLY_INTEGER) {
		fprintf(stderr, "[-][GMS_REDIS]Set key:%s delete time ERROR!\n", key);
		log_(L_INFO | L_CONS,
				"[-][GMS_REDIS]Set key:%s delete time ERROR! %s\n", key,
				conn->errstr);
		retn = -1;
	}
	if (reply->integer == 1) {
		/* 成功 */
		retn = 0;
		log_(L_INFO | L_CONS,
				"[+][GMS_REDIS]EXPIRE Set key:%s delete time Succse!\n", key);
	} else {
		/* 错误 */
		retn = -1;
	}

	*rep = reply;
	return retn;
}

/* -------------------------------------------*/
/**
 * @brief  打印库中所有匹配pattern的key
 *
 * @param conn		已建立好的链接
 * @param pattern	匹配模式，pattern支持glob-style的通配符格式，
 *					如 *表示任意一个或多个字符，
 *					   ?表示任意字符，
 *				    [abc]表示方括号中任意一个字母。
 */
/* -------------------------------------------*/
void rop_show_keys(redisContext *conn, void **rep, char* pattern) {
	unsigned int i = 0;
	redisReply *reply = NULL;

	reply = (redisReply *) redisCommand(conn, "keys %s", pattern);
	if (reply->type != REDIS_REPLY_ARRAY) {
		fprintf(stderr, "[-][GMS_REDIS]show all keys and data wrong type!\n");
		log_(L_INFO | L_CONS,
				"[-][GMS_REDIS]show all keys and data wrong type! %s\n",
				conn->errstr);
		goto END;
	}

	for (i = 0; i < reply->elements; ++i) {
		printf("======[%s]======\n", reply->element[i]->str);
	}

	END: *rep = reply;
}

/* -------------------------------------------*/
/**
 * @brief  测试一个reply的结果类型
 *			得到对应的类型用对应的方法获取数据
 *
 * @param reply		返回的命令结果
 */
/* -------------------------------------------*/
void rop_test_reply_type(redisReply *reply) {
	switch (reply->type) {
	case REDIS_REPLY_STATUS:
		log_(L_INFO | L_CONS,
				"[+][GMS_REDIS]=REDIS_REPLY_STATUS=[string] use reply->str to get data, reply->len get data len\n");
		break;
	case REDIS_REPLY_ERROR:
		log_(L_INFO | L_CONS,
				"[+][GMS_REDIS]=REDIS_REPLY_ERROR=[string] use reply->str to get data, reply->len get date len\n");
		break;
	case REDIS_REPLY_INTEGER:
		log_(L_INFO | L_CONS,
				"[+][GMS_REDIS]=REDIS_REPLY_INTEGER=[long long] use reply->integer to get data\n");
		break;
	case REDIS_REPLY_NIL:
		log_(L_INFO | L_CONS,
				"[+][GMS_REDIS]=REDIS_REPLY_NIL=[] data not exist\n");
		break;
	case REDIS_REPLY_ARRAY:
		log_(L_INFO | L_CONS,
				"[+][GMS_REDIS]=REDIS_REPLY_ARRAY=[array] use reply->elements to get number of data, reply->element[index] to get (struct redisReply*) Object\n");
		break;
	case REDIS_REPLY_STRING:
		log_(L_INFO | L_CONS,
				"[+][GMS_REDIS]=REDIS_REPLY_string=[string] use reply->str to get data, reply->len get data len\n");
		break;
	default:
		log_(L_INFO | L_CONS, "[-][GMS_REDIS]Can't parse this type\n");
		break;
	}
}

int rop_hash_add(redisContext *conn, void **rep, const char* key,
		const char* member, const char* vul) {
	int retn = 0;

	redisReply *reply = NULL;

	reply = (redisReply *) redisCommand(conn, "HSET  %s %s %s", key, member,
			vul);
	//rop_test_reply_type(reply);
	if (reply->type != REDIS_REPLY_INTEGER) {
		log_(L_INFO | L_CONS,
				"[-][GMS_REDIS]HSET  hash: %s,member: %s Error:%s,%s\n", key,
				member, reply->str, conn->errstr);

		retn = -1;
		goto END;
	}
	log_(L_INFO | L_CONS, "[+][GMS_REDIS]HSET  hash: %s,member: %s Succse:%d\n",
			key, member, reply->integer);

	END: *rep = reply;
	return retn;
}

int rop_hash_get(redisContext *conn, void **rep, const char* key,
		const char* member) {
	int retn = 0;

	redisReply *reply = NULL;

	reply = (redisReply *) redisCommand(conn, "HGET  %s %s ", key, member);
	//rop_test_reply_type(reply);
	if (reply->type != REDIS_REPLY_STRING) {
		log_(L_INFO | L_CONS, "[-][GMS_REDIS]HGET %s,%s error %s\n", key,
				member, conn->errstr);
		retn = -1;
		goto END;
	}
	log_(L_INFO | L_CONS, "[+][GMS_REDIS]HGET %s,%s SUCCESS %s\n", key, member,
			reply->str);
	//    strcpy(buf,reply->str);
	//    buf[reply->len]='\0';

	END: *rep = reply;
	return retn;
}

/* -------------------------------------------*/
/**
 * @brief  给指定的hash表 指定的field对应的value自增num
 *
 * @param conn			已建立好的链接
 * @param key			hash表名
 * @param field			hash表下的区域名
 *
 * @returns
 *			0		succ
 *			-1		fail
 */
/* -------------------------------------------*/
int rop_hincrement_one_field(redisContext *conn, void **rep, char *key,
		char *field, unsigned int num) {
	int retn = 0;

	redisReply *reply = NULL;

	reply = (redisReply *) redisCommand(conn, "HINCRBY %s %s %d", key, field,
			num);
	if (reply == NULL) {
		log_(L_INFO | L_CONS, "[-][GMS_REDIS]increment %s %s error %s\n", key,
				field, conn->errstr);
		retn = -1;
		goto END;
	}

	END: *rep = reply;

	return retn;
}


/* -------------------------------------------*/
/**
 * @brief  单条数据插入链表
 *
 * @param conn		已建立好的链接
 * @param key		链表名
 * @param value		数据
 *
 * @returns
 */
/* -------------------------------------------*/
int rop_list_push(redisContext *conn, void **rep, const char *key,
		const char *value) {
	int retn = 0;
	redisReply *reply = NULL;

	reply = (redisReply *) redisCommand(conn, "LPUSH %s %s", key, value);
	//rop_test_reply_type(reply);
	if (reply->type != REDIS_REPLY_INTEGER) {
		log_(L_INFO | L_CONS, "[-][GMS_REDIS]LPUSH %s %s error!%s\n", key,
				value, conn->errstr);
		retn = -1;
	} else {
		log_(L_INFO | L_CONS, "[+][GMS_REDIS]LPUSH %s %s SUCCESS!\n", key,
				value);
	}
	*rep = reply;
	return retn;
}

int rop_list_pop(redisContext *conn, void **rep, const char *key) {
	int retn = 0;
	redisReply *reply = NULL;

	reply = (redisReply *) redisCommand(conn, "LPOP %s", key);
	//rop_test_reply_type(reply);
	if (reply->type != REDIS_REPLY_STRING) {
		log_(L_INFO | L_CONS, "[-][GMS_REDIS]LPOP %s error %s\n", key,
				conn->errstr);
		retn = -1;
	} else {
		log_(L_INFO | L_CONS, "[+][GMS_REDIS]LPOP %s SUCCESS %s\n", key,
				reply->str);
	}
	*rep = reply;
	return retn;
}

/* -------------------------------------------*/
/**
 * @brief  得到链表中元素的个数
 *
 * @param conn	链接句柄
 * @param key	链表名
 *
 * @returns
 *			>=0 个数
 *			-1 fail
 */
/* -------------------------------------------*/
int rop_get_list_cnt(redisContext *conn, void ** rep, const char *key) {
	int cnt = 0;

	redisReply *reply = NULL;

	reply = (redisReply *) redisCommand(conn, "LLEN %s", key);
	if (reply->type != REDIS_REPLY_INTEGER) {
		log_(L_INFO | L_CONS, "[-][GMS_REDIS]LLEN %s error %s\n", key,
				conn->errstr);
		cnt = -1;
		goto END;
	}

	cnt = reply->integer;
	log_(L_INFO | L_CONS, "[+][GMS_REDIS]LLEN %s SUCCES %d\n", key, cnt);

	END: *rep = reply;
	return cnt;
}

/* -------------------------------------------*/
/**
 * @brief  按照一定范围截断链表中的数据
 *
 * @param conn		已经建立的链接
 * @param key		链表名
 * @param begin		阶段启示位置 从 0 开始
 * @param end		阶段结束位置 从 -1 开始
 *
 *					这里的范围定义举例
 *					如果得到全部范围(0, -1)
 *					除了最后一个元素范围(0, -2)
 *					前20各数据范围(0, 19)
 *
 * @returns
 *			0  SUCC
 *			-1 FAIL
 */
/* -------------------------------------------*/
int rop_trim_list(redisContext *conn, void **rep, const char *key, int begin,
		int end) {
	int retn = 0;
	redisReply *reply = NULL;

	reply = (redisReply *) redisCommand(conn, "LTRIM %s %d %d", key, begin,
			end);
	if (reply->type != REDIS_REPLY_STATUS) {
		log_(L_INFO | L_CONS, "[-][GMS_REDIS]LTRIM %s %d %d error!%s\n", key,
				begin, end, conn->errstr);
		retn = -1;
	} else {
		log_(L_INFO | L_CONS, "[+][GMS_REDIS]LTRIM %s %d %d SUSSCE!\n", key,
				begin, end);
	}

	*rep = reply;
	return retn;
}

/* -------------------------------------------*/
/**
 * @brief  rop_range_list 得到链表中的数据
 *
 *          返回数据为 区间为
 *              [from_pos, end_pos)
 *
 * @param conn
 * @param key       表名
 * @param from_pos  查找表的起始数据下标
 * @param end_pos   查找表的结尾数据下标
 * @param values    得到表中的value数据
 * @param get_num   得到结果value的个数
 *
 * @returns
 *      0 succ, -1 fail
 */
/* -------------------------------------------*/
int rop_range_list(redisContext *conn, void **rep, const char *key,
		int from_pos, int end_pos) {
	int retn = 0;
	int i = 0;
	redisReply *reply = NULL;
	int max_count = 0;

	unsigned int count = end_pos - from_pos + 1;

	reply = (redisReply *) redisCommand(conn, "LRANGE %s %d %d", key, from_pos,
			end_pos);
//    rop_test_reply_type(reply);
	if (reply->type != REDIS_REPLY_ARRAY) {
		log_(L_INFO | L_CONS, "[-][GMS_REDIS]LRANGE %s  error!%s\n", key,
				conn->errstr);
		retn = -1;
	}

	max_count = (reply->elements > count) ? count : reply->elements;
//    *get_num = max_count;
	log_(L_INFO | L_CONS, "[+][GMS_REDIS]LRANGE %s  count= %d \n", key,
			max_count);

	for (i = 0; i < max_count; ++i) {
		log_(L_INFO | L_CONS, "[+][GMS_REDIS]LRANGE %s  indes: %d = %s\n", key,
				i, reply->element[i]->str);
//        strncpy(values[i], reply->element[i]->str, VALUES_ID_SIZE-1);
	}

	*rep = reply;
	return retn;
}

/* -------------------------------------------*/
/**
 * @brief		将指定的zset表，对应的成员，值自增1
 *				（key 或 成员不存在 则创建）
 *
 * @param conn		已建立的链接
 * @param key		zset表名
 * @param member	zset成员名
 *
 * @returns
 *			0			succ
 *			-1			fail
 */
/* -------------------------------------------*/
int rop_zset_increment(redisContext *conn, void **rep, const char* key,
		const char* member) {
	int retn = 0;

	redisReply *reply = NULL;

	reply = (redisReply *) redisCommand(conn, "ZINCRBY %s 10 %s", key, member);
	//rop_test_reply_type(reply);
	if (reply->type != REDIS_REPLY_STRING) {
		log_(L_INFO | L_CONS,
				"[-][GMS_REDIS]Add or increment table: %s,member: %s Error:%s,%s\n",
				key, member, reply->str, conn->errstr);

		retn = -1;
		goto END;
	}
	log_(L_INFO | L_CONS,
			"[+][GMS_REDIS]Add or increment table: %s,member: %s Succse:%s,%s\n",
			key, member, reply->str, conn->errstr);

	END: *rep = reply;
	return retn;
}

int rop_zset_add(redisContext *conn, void **rep, const char* key,
		const char* member, int score) {
	int retn = 0;

	redisReply *reply = NULL;

	reply = (redisReply *) redisCommand(conn, "ZADD %s %d %s", key, score,
			member);
	//rop_test_reply_type(reply);
	if (reply->type != REDIS_REPLY_INTEGER) {
		log_(L_INFO | L_CONS,
				"[-][GMS_REDIS]ZAdd  table: %s,member: %s Error:%s,%s\n", key,
				member, reply->str, conn->errstr);

		retn = -1;
		goto END;
	}
	log_(L_INFO | L_CONS,
			"[+][GMS_REDIS]ZAdd  table: %s,member: %s Succse:%d\n", key, member,
			reply->integer);

	END: *rep = reply;
	return retn;
}

int rop_zset_remove(redisContext *conn, void **rep, const char* key,
		const char* member) {
	int retn = 0;

	redisReply *reply = NULL;

	reply = (redisReply *) redisCommand(conn, "ZREM %s %s", key, member);
	//rop_test_reply_type(reply);
	if (reply->type != REDIS_REPLY_INTEGER) {
		log_(L_INFO | L_CONS,
				"[-][GMS_REDIS]ZREM  table: %s,member: %s Error:%s,%s\n", key,
				member, reply->str, conn->errstr);

		retn = -1;
		goto END;
	}
	log_(L_INFO | L_CONS,
			"[+][GMS_REDIS]ZREM  table: %s,member: %s Succse:%d\n", key, member,
			reply->integer);

	END: *rep = reply;
	return retn;
}

/**
 * 返回有序集 key 中，成员 member 的 score 值。
 *	如果 member 元素不是有序集 key 的成员，或 key 不存在，返回 nil 。
 */
int rop_zset_get_score(redisContext *conn, void **rep, const char *key,
		const char *member) {
	int score = 0;

	redisReply *reply = NULL;

	reply = (redisReply *) redisCommand(conn, "ZSCORE %s %s", key, member);
	rop_test_reply_type(reply);

	if (reply->type != REDIS_REPLY_STRING) {
		log_(L_INFO | L_CONS, "[-][GMS_REDIS]ZSCORE %s %s error %s\n", key,
				member, conn->errstr);
		score = -1;
		goto END;
	}

	score = atoi(reply->str);
	log_(L_INFO | L_CONS, "[+][GMS_REDIS]ZSCORE %s %s SUCCESS %s\n", key,
			member, reply->str);

	END: *rep = reply;

	return score;
}

int rop_set_add(redisContext *conn, void **rep, const char* key,
		const char* member) {
	int retn = 0;

	redisReply *reply = NULL;

	reply = (redisReply *) redisCommand(conn, "SADD %s %s", key, member);
	//rop_test_reply_type(reply);
	if (reply->type != REDIS_REPLY_INTEGER) {
		log_(L_INFO | L_CONS,
				"[-][GMS_REDIS]SADD  table: %s,member: %s Error:%s,%s\n", key,
				member, reply->str, conn->errstr);

		retn = -1;
		goto END;
	}
	log_(L_INFO | L_CONS,
			"[+][GMS_REDIS]SADD  table: %s,member: %s Succse:%d\n", key, member,
			reply->integer);

	END: *rep = reply;
	return retn;
}

int rop_set_remove(redisContext *conn, void **rep, const char* key,
		const char* member) {
	int retn = 0;

	redisReply *reply = NULL;

	reply = (redisReply *) redisCommand(conn, "SREM %s %s", key, member);
	//rop_test_reply_type(reply);
	if (reply->type != REDIS_REPLY_INTEGER) {
		log_(L_INFO | L_CONS,
				"[-][GMS_REDIS]SREM  table: %s,member: %s Error:%s,%s\n", key,
				member, reply->str, conn->errstr);

		retn = -1;
		goto END;
	}
	log_(L_INFO | L_CONS,
			"[+][GMS_REDIS]SREM  table: %s,member: %s Succse:%d\n", key, member,
			reply->integer);

	END: *rep = reply;
	return retn;
}

/* -------------------------------------------*/
/**
 * @brief       插入字符串键值对
 *
 * @param conn		已建立的链接
 * @param key		键
 * @param value		值
 *
 * @returns
 *			0			succ
 *			-1			fail
 */
/* -------------------------------------------*/
int rop_string_set(redisContext *conn, void **rep, const char* key,
		const char* value) {
	int retn = 0;
	redisReply *reply = NULL;

	reply = (redisReply *) redisCommand(conn, "SET %s %s", key, value);
	//测试返回值的类型
	if (reply->type != REDIS_REPLY_STATUS) {
		log_(L_INFO | L_CONS, "[-][GMS_REDIS]SET %s %s error %s\n", key, value,
				reply->str, conn->errstr);
		retn = -1;
		goto END;
	}
	log_(L_INFO | L_CONS, "[+][GMS_REDIS]SET %s SUCCESS %s\n", key, reply->str);

	END: *rep = reply;
	return retn;
}

/* -------------------------------------------*/
/**
 * @brief       获取字符串键值对
 *
 * @param conn		已建立的链接
 * @param key		键
 * @param value		buf
 *
 *
 * @returns
 *			0			succ
 *			-1			fail
 */
/* -------------------------------------------*/
int rop_string_get(redisContext *conn, void **rep,
		const char* key/*,char* buf*/) {
	int retn = 0;
	redisReply *reply = NULL;

	reply = (redisReply *) redisCommand(conn, "GET %s", key);
	//测试返回值的类型
	if (reply->type != REDIS_REPLY_STRING) {
		log_(L_INFO | L_CONS, "[-][GMS_REDIS]GET %s error %s\n", key,
				conn->errstr);
		retn = -1;
		goto END;
	}
	log_(L_INFO | L_CONS, "[+][GMS_REDIS]GET %s SUCCESS %s\n", key, reply->str);
//    strcpy(buf,reply->str);
//    buf[reply->len]='\0';

	END: *rep = reply;
	return retn;
}

int main(int argc, char** argv) {
	(void) argc;
	(void) argv;

	LOG_CONFIG log = { -1, LOG_DEST_FILES, "log/test_hiredispool.log",
			"test_hiredispool", 0, 1 };
	log_set_config(&log);

	REDIS_ENDPOINT endpoints[2] =
//			{ { "127.0.0.1", 6379 }, { "127.0.0.1", 6379 }
			{ { "132.122.232.179", 7352 }, { "132.122.232.180", 7353 }, };

	REDIS_CONFIG conf = { (REDIS_ENDPOINT*) &endpoints, 2, 10000, 5000, 2, 10,
			1, "test001#Abc12345!",
			};

	REDIS_INSTANCE* inst;
	if (redis_pool_create(&conf, &inst) < 0)
		return -1;

//    sleep(5);

	//连接准备--------------------------------------------------------------------
	REDIS_SOCKET* sock_str;
	if ((sock_str = redis_get_socket(inst)) == NULL) {
		redis_pool_destroy(inst);
		return -1;
	}
	redisReply * reply = NULL;
	rop_selectdatabase((redisContext *) sock_str->conn, (void **) &reply, 4970);

	//---------------string------------------
	log_(L_INFO | L_CONS, "---------------string------------------");
	rop_string_set((redisContext *) sock_str->conn, (void **) &reply, "str_key",
			"str_vul");

	rop_string_get((redisContext *) sock_str->conn, (void **) &reply,
			"str_key");

	rop_del_key((redisContext *) sock_str->conn, (void **) &reply, "str_key");

	rop_string_get((redisContext *) sock_str->conn, (void **) &reply,
			"str_key");

	//二进制

	reply = (redisReply *) redisCommand((redisContext *)sock_str->conn, "SET %s %b", "str_key_bin", "value",4);
	//测试返回值的类型
	if (reply->type != REDIS_REPLY_STATUS) {
		log_(L_INFO | L_CONS, "[-][GMS_REDIS]SET bin error ");
	}
	log_(L_INFO | L_CONS, "[+][GMS_REDIS]SET bin %s SUCCESS %s\n", "str_key_bin", reply->str);

	//前面长度为4，所以获取值为valu而不是value
	rop_string_get((redisContext *) sock_str->conn, (void **) &reply,
				"str_key_bin");

	//设置过期时间
	rop_string_set((redisContext *) sock_str->conn, (void **) &reply,
			"str_key2", "str_vul2");
	rop_string_get((redisContext *) sock_str->conn, (void **) &reply,
			"str_key2");
	rop_set_key_lifecycle((redisContext *) sock_str->conn, (void **) &reply,
			"str_key2", 1);

	rop_is_key_exist((redisContext *) sock_str->conn, (void **) &reply,"str_key2");
	sleep(2);
	rop_string_get((redisContext *) sock_str->conn, (void **) &reply,
			"str_key2");
	rop_is_key_exist((redisContext *) sock_str->conn, (void **) &reply,"str_key2");

	//---------------list---------------
	log_(L_INFO | L_CONS, "---------------list------------------");

	REDIS_SOCKET* sock_list;
	if ((sock_list = redis_get_socket(inst)) == NULL) {
		redis_pool_destroy(inst);
		return -1;
	}
	redisReply * reply_list = NULL;
	rop_selectdatabase((redisContext *) sock_list->conn, (void **) &reply_list,
			4970);

	//入队列
	rop_list_push((redisContext *) sock_list->conn, (void **) &reply_list,
			"list_key", "list_1");
	rop_list_push((redisContext *) sock_list->conn, (void **) &reply_list,
			"list_key", "list_2");
	rop_list_push((redisContext *) sock_list->conn, (void **) &reply_list,
			"list_key", "list_3");

	//获取list节点数量
	rop_get_list_cnt((redisContext *) sock_list->conn, (void **) &reply_list,
			"list_key");

	//获取范围内数据
	rop_range_list((redisContext *) sock_list->conn, (void **) &reply_list,
			"list_key", 0, 11);

	//出队列
	rop_list_pop((redisContext *) sock_list->conn, (void **) &reply_list,
			"list_key");

	//截取队列
	rop_trim_list((redisContext *) sock_list->conn, (void **) &reply_list,
			"list_key", 0, 5);
	rop_get_list_cnt((redisContext *) sock_list->conn, (void **) &reply_list,
			"list_key");
	rop_range_list((redisContext *) sock_list->conn, (void **) &reply_list,
			"list_key", 0, 11);

	//---------------set---------------
	log_(L_INFO | L_CONS, "---------------set------------------");
	REDIS_SOCKET* sock_set;
	if ((sock_set = redis_get_socket(inst)) == NULL) {
		redis_pool_destroy(inst);
		return -1;
	}
	redisReply * reply_set = NULL;
	rop_selectdatabase((redisContext *) sock_set->conn, (void **) &reply_set,
			4970);

	rop_set_add((redisContext *) sock_set->conn, (void **) &reply_set,
			"set_key", "baidu");
	rop_set_add((redisContext *) sock_set->conn, (void **) &reply_set,
			"set_key", "google");
	rop_set_add((redisContext *) sock_set->conn, (void **) &reply_set,
			"set_key", "facebook");

	rop_set_remove((redisContext *) sock_set->conn, (void **) &reply_set,
			"set_key", "facebook");

	//---------------Zset---------------
	log_(L_INFO | L_CONS, "---------------Zset------------------");
	REDIS_SOCKET* sock_Zset;
	if ((sock_Zset = redis_get_socket(inst)) == NULL) {
		redis_pool_destroy(inst);
		return -1;
	}
	redisReply * reply_Zset = NULL;
	rop_selectdatabase((redisContext *) sock_Zset->conn, (void **) &reply_Zset,
			4970);

	rop_zset_add((redisContext *) sock_Zset->conn, (void **) &reply_Zset,
			"zset_key", "baidu", 10);
	rop_zset_add((redisContext *) sock_Zset->conn, (void **) &reply_Zset,
			"zset_key", "google", 500);
	rop_zset_add((redisContext *) sock_Zset->conn, (void **) &reply_Zset,
			"zset_key", "facebook", 200);

	rop_zset_increment((redisContext *) sock_Zset->conn, (void **) &reply_Zset,
			"zset_key", "facebook");
	rop_zset_get_score((redisContext *) sock_Zset->conn, (void **) &reply_Zset,
			"zset_key", "facebook");

	rop_zset_remove((redisContext *) sock_Zset->conn, (void **) &reply_Zset,
			"zset_key", "baidu");
	rop_zset_remove((redisContext *) sock_Zset->conn, (void **) &reply_Zset,
			"zset_key", "google");
	rop_zset_remove((redisContext *) sock_Zset->conn, (void **) &reply_Zset,
			"zset_key", "facebook");

	rop_zset_get_score((redisContext *) sock_Zset->conn, (void **) &reply_Zset,
			"zset_key", "facebook");

	//---------------hash---------------
	log_(L_INFO | L_CONS, "---------------hash------------------");
	REDIS_SOCKET* sock_hash;
	if ((sock_hash = redis_get_socket(inst)) == NULL) {
//		redis_release_socket(reply, inst, sock_str);//模拟连接池数量不够的情况
//			redis_release_socket(reply_list, inst, sock_list);
//			redis_release_socket(reply_set, inst, sock_set);
//			redis_release_socket(reply_Zset, inst, sock_Zset);
		redis_pool_destroy(inst);
		return -1;
	}
	redisReply * reply_hash = NULL;
	rop_selectdatabase((redisContext *) sock_hash->conn, (void **) &reply_hash,
			4970);

	rop_hash_add((redisContext *) sock_hash->conn, (void **) &reply_hash,"hash_key", "one", "1");
	rop_hash_add((redisContext *) sock_hash->conn, (void **) &reply_hash,"hash_key", "two", "2");
	rop_hash_add((redisContext *) sock_hash->conn, (void **) &reply_hash,"hash_key", "three", "3");

	rop_hash_get((redisContext *) sock_hash->conn, (void **) &reply_hash,"hash_key", "three");

	//append command
	log_(L_INFO | L_CONS, "---------------Append command------------------");
		REDIS_SOCKET* sock_Append;
		if ((sock_Append = redis_get_socket(inst)) == NULL) {
			redis_pool_destroy(inst);
			return -1;
		}

	redisReply * reply_Append = NULL;
	rop_selectdatabase((redisContext *) sock_Append->conn,
			(void **) &reply_Append, 4970);


	redisAppendCommand((redisContext *) sock_Append->conn, "set foo foo");
	redisAppendCommand((redisContext *) sock_Append->conn, "set t tt");
	redisAppendCommand((redisContext *) sock_Append->conn, "set a aa");
	redisAppendCommand((redisContext *) sock_Append->conn, "get foo");
	redisAppendCommand((redisContext *) sock_Append->conn, "get a");
	redisAppendCommand((redisContext *) sock_Append->conn, "get t");

	for (int i = 0; i < 6; ++i) {
		int r = redisGetReply((redisContext *) sock_Append->conn,
				(void **) &reply_Append);
		if (r == REDIS_ERR) {
			printf("ERROR\n");
			//			freeReplyObject(reply);
		} else {
			printf("res: %s, num: %zu, type: %d\n", reply_Append->str, reply_Append->elements,
					reply_Append->type);
		}
	}



	//归还连接需要传入最后一次reply
	redis_release_socket(reply, inst, sock_str);
	redis_release_socket(reply_list, inst, sock_list);
	redis_release_socket(reply_set, inst, sock_set);
	redis_release_socket(reply_Zset, inst, sock_Zset);
	redis_release_socket(reply_hash, inst, sock_hash);
	redis_release_socket(reply_Append, inst, sock_Append);
	redis_pool_destroy(inst);

	//归还连接后在进行freeReplyObject
	freeReplyObject(reply);
	freeReplyObject(reply_list);
	freeReplyObject(reply_set);
	freeReplyObject(reply_Zset);
	freeReplyObject(reply_hash);
	freeReplyObject(reply_Append);

	return 0;
}

