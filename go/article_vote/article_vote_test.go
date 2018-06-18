package redis_in_action_test

import (

	"testing"

	"github.com/gomodule/redigo/redis"

	"time"
	"fmt"
	"math/rand"
	"errors"
	"encoding/json"
)

type ID uint32


type articleInfo struct {
	Id         string
	Title      string
	Link       string
	Poster     string
	CreateTime int64
}

type user struct{
	name string
}

const (
	//涉及到的表
	Table_ArticleId = "articleId"  //哈希
	Table_ArticleInfo = "articleInfo" //哈希
	Table_ArticleVotedNum = "articleVotedNum"  //有序集合

	//涉及到的表前缀
	Prefix_VotedSet = "voted:"   //Prefix_VotedSet + articleId:xxxx，集合

	//基本规格参数
	Vote_Expire_Time = 10  //为便于测试，暂定10s
)

var (
	ErrRedisDoFail = errors.New("Redis Do Failed")
	ErrArticleIsExpired = errors.New("Article is expired")
)

func (u *user) postArticle(c redis.Conn, title, link string) error{
	//获取文章id
	id, err := redis.Uint64(c.Do("INCR", Table_ArticleId))
	if err != nil{
		fmt.Println("Get ID Fail", err)
		return errors.New("Get article ID failed.")
	}

	artId := fmt.Sprintf("articleId:%d", id)
	//提交文章到redis
	art := articleInfo{Id:artId, Title:title, Link:link, Poster:u.name, CreateTime:time.Now().Unix()}
	value,err := json.Marshal(art)

	_, err = c.Do("HSET", Table_ArticleInfo, artId, value)
	if err != nil{
		fmt.Println("hset fail", err)
		return ErrRedisDoFail
	}
	fmt.Println("user ", u.name, " post an article. Id is ", artId)

	return nil
}

func (u *user) voteAritcle(c redis.Conn, voteArtId string) error{
	var artInfo articleInfo
	r, err := redis.Bytes(c.Do("HGET", Table_ArticleInfo, voteArtId))
	if err != nil{
		return ErrRedisDoFail
	}
	json.Unmarshal(r, &artInfo)
	if artInfo.CreateTime + Vote_Expire_Time < time.Now().Unix() {
		return ErrArticleIsExpired
	}

	votedSet := Prefix_VotedSet + voteArtId
	_, err = c.Do("SADD", votedSet, u.name)
	if err != nil{
		fmt.Println("hset fail", err)
		return ErrRedisDoFail
	}
	c.Do("EXPIRE",votedSet,Vote_Expire_Time)
	c.Do("ZADD", Table_ArticleVotedNum, "INCR", 1, voteArtId)

	fmt.Println(u.name, "vote to ", votedSet)
	return nil
}

func (u *user) getTopVotedArticleIds(c redis.Conn, num uint32)[]string{
	if num == 0{
		return  nil
	}

	v, err := redis.Strings(c.Do("ZREVRANGE",  Table_ArticleVotedNum, 0, num-1, "WITHSCORES"))
	if err != nil{
		fmt.Println("Get all keys fail", err, v)
		return nil
	}
	fmt.Println("Get top articles", len(v), v)
	return v
}

//此处只是为了测试获取数据用，直接获取全量会有性能、内存问题。
func (u *user) getAllArticleIds(c redis.Conn)[]string{
	v, err := redis.Strings(c.Do("HKEYS",  Table_ArticleInfo))
	if err != nil{
		fmt.Println("Get all keys fail", err, v)
		return nil
	}
	//fmt.Println("Get all article", reflect.TypeOf(v),len(v), v)
	return v
}

func TestWrite(t *testing.T) {
	server := "127.0.0.1:6379"

	c, err := redis.Dial("tcp", server)
	if err != nil {
		fmt.Println("connect server failed:", err)
		return
	}

	//清空测试数据
	defer func() {
		time.Sleep(time.Second)
		fmt.Println("Begin to clear table.")
		clrUser := user{"ClearUser"}
		clrIds := clrUser.getAllArticleIds(c)
		for _, Id := range clrIds{
			c.Do("DEL", Prefix_VotedSet+Id)
		}
		c.Do("DEL", Table_ArticleId)
		c.Do("DEL", Table_ArticleInfo)
		c.Do("DEL", Table_ArticleVotedNum)

		c.Close()
	}()

	//创建100篇文章
	func(){
		for i := 0; i < 100; i++{
			u := user{fmt.Sprintf("User%d",i)}
			u.postArticle(c, "Title"+string(i), "www.xxx"+string(i))
		}
	}()

	//随机投票
	func(){
		for i := 0; i < 100; i++{
			u := user{fmt.Sprintf("User%d",i)}
			artIds := u.getAllArticleIds(c)
			voteId := artIds[rand.Intn(len(artIds))]
			fmt.Println(u, " vote to ", voteId)
			u.voteAritcle(c, voteId)
		}
	}()

	//展示top投票文章
	u := user{"TestUser"}
	u.getTopVotedArticleIds(c, 10)
}
