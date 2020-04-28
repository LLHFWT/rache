package conf

import (
	"encoding/json"
	"io/ioutil"
)

//定义一个全局配置对象
type GlobalConfig struct {
	//Master 监听的地址
	MasterAddr []string
	// KV Group监听的地址
	GroupAddr   [][]string
	MaxRaftSize int
}

//读取用户的配置文件参数
func (g *GlobalConfig) Reload() {
	data, err := ioutil.ReadFile("./conf/conf.json")
	if err != nil {
		panic(err)
	}
	//将json数据解析到struct中
	err = json.Unmarshal(data, &GlobalConfigObj)
	if err != nil {
		panic(err)
	}
}

//供全局使用的配置信息对象
var GlobalConfigObj *GlobalConfig

//初始化参数加载
func init() {
	//初始化GlobalObject变量，并设置一些初始化参数
	GlobalConfigObj = &GlobalConfig{}
	//从配置文件中加载一些用户参数
	GlobalConfigObj.Reload()
}
