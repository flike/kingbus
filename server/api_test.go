package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/flike/kingbus/config"
	"github.com/flike/kingbus/utils"
	"github.com/stretchr/testify/require"
)

var follower *KingbusServer

func TestBinlogServerHandler_StartBinlogServer(t *testing.T) {
	StartBsInHandlerForTest(t)
	StopClusterInTest(cluster)
}

func StartBsInHandlerForTest(t *testing.T) {
	var httpResp utils.Resp

	InitClusterForTest(t)
	for i := 0; i < len(cluster); i++ {
		if cluster[i] != leader {
			follower = cluster[i]
			break
		}
	}

	url := fmt.Sprintf("http://%s/binlog/server/start", follower.adminSvr.AdminAddr)
	t.Logf("start binlog server url:%s", url)
	contentType := "application/json;charset=utf-8"

	args := config.BinlogServerConfig{
		Addr:     "127.0.0.1:3309",
		User:     "kingbus",
		Password: "123456",
	}

	b, err := json.Marshal(args)
	require.Nil(t, err)
	body := bytes.NewBuffer(b)

	client := &http.Client{}
	req, err := http.NewRequest("PUT", url, body)
	require.Nil(t, err)
	req.Header.Set("Content-Type", contentType)

	resp, err := client.Do(req)
	require.Nil(t, err)
	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	require.Nil(t, err)

	d := json.NewDecoder(bytes.NewReader(respBody))
	d.UseNumber()
	err = d.Decode(&httpResp)
	require.Nil(t, err)
	require.Equal(t, httpResp.Message, "success")

	url = fmt.Sprintf("http://%s/binlog/server/status", follower.adminSvr.AdminAddr)
	t.Logf("http.Get success,url:%s", url)
	resp, err = http.Get(url)
	require.Nil(t, err)

	defer resp.Body.Close()
	respBody, err = ioutil.ReadAll(resp.Body)
	require.Nil(t, err)

	d = json.NewDecoder(bytes.NewReader(respBody))
	d.UseNumber()
	err = d.Decode(&httpResp)
	require.Nil(t, err)

	require.Equal(t, "success", httpResp.Message)
	t.Logf("binlog master status:%s", httpResp.Data)
}

func TestBinlogServerHandler_GetBinlogServerStatus(t *testing.T) {
	var httpResp utils.Resp

	StartBsInHandlerForTest(t)
	defer StopBinlogServerForTest(t)

	url := fmt.Sprintf("http://%s/binlog/server/status", follower.adminSvr.AdminAddr)
	t.Logf("http.Get success,url:%s", url)
	resp, err := http.Get(url)
	require.Nil(t, err)

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	require.Nil(t, err)

	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	err = d.Decode(&httpResp)
	require.Nil(t, err)

	require.Equal(t, "success", httpResp.Message)
	t.Logf("binlog master status:%s", httpResp.Data)
}

func TestBinlogSyncerHandler_StartBinlogSyncer(t *testing.T) {

}

func TestBinlogSyncerHandler_StopBinlogSyncer(t *testing.T) {

}

func TestBinlogSyncerHandler_GetBinlogSyncerStatus(t *testing.T) {

}

func TestGetMembers(t *testing.T) {
	var httpResp utils.Resp
	cluster := StartClusterInTest()
	defer StopClusterInTest(cluster)

	resp, err := http.Get("http://127.0.0.1:9591/members")
	require.Nil(t, err)

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	require.Nil(t, err)

	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	err = d.Decode(&httpResp)
	require.Nil(t, err)
	t.Logf("get resp:%s", httpResp.Message)
	require.Equal(t, "success", httpResp.Message)

	members, ok := httpResp.Data.([]interface{})
	require.Equal(t, true, ok)
	for i := 0; i < len(members); i++ {
		member, ok := members[i].(map[string]interface{})
		require.Equal(t, true, ok)
		t.Logf("id:%s,peerURLs:%s,name:%s", member["Id"], member["peerURLs"], member["name"])
	}
}

func TestUpdateMember(t *testing.T) {
	var httpResp utils.Resp
	cluster := StartClusterInTest()
	defer StopClusterInTest(cluster)

	url := fmt.Sprintf("http://%s/members", follower.adminSvr.AdminAddr)
	contentType := "application/json;charset=utf-8"
	args := struct {
		NodeName string `json:"name"`
		PeerURL  string `json:"new_peer_url"`
	}{NodeName: follower.Cfg.RaftNodeCfg.Name, PeerURL: "http://127.0.0.1:15009"}
	b, err := json.Marshal(args)
	require.Nil(t, err)
	body := bytes.NewBuffer(b)
	client := &http.Client{}
	req, err := http.NewRequest("PUT", url, body)
	require.Nil(t, err)
	req.Header.Set("Content-Type", contentType)

	resp, err := client.Do(req)
	require.Nil(t, err)
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	require.Nil(t, err)

	d := json.NewDecoder(bytes.NewReader(respBody))
	d.UseNumber()
	err = d.Decode(&httpResp)
	require.Nil(t, err)
	require.Equal(t, "success", httpResp.Message)
	members, ok := httpResp.Data.([]interface{})
	require.Equal(t, true, ok)

	var exist = false
	for i := 0; i < len(members); i++ {
		member, ok := members[i].(map[string]interface{})
		require.Equal(t, true, ok)
		if member["name"].(string) == args.NodeName && member["peerURLs"].([]interface{})[0].(string) == args.PeerURL {
			exist = true
			break
		}
		t.Logf("Id:%s,peerURLs:%s,name:%s", member["Id"], member["peerURLs"], member["name"])
	}
	require.Equal(t, true, exist)
}

func TestDeleteMember(t *testing.T) {
	var httpResp utils.Resp
	cluster := StartClusterInTest()
	defer StopClusterInTest(cluster)

	url := fmt.Sprintf("http://%s/members", follower.adminSvr.AdminAddr)
	contentType := "application/json;charset=utf-8"

	args := struct {
		NodeName string `json:"name"`
		PeerURL  string `json:"peer_url"`
	}{NodeName: "node_1", PeerURL: "http://127.0.0.1:15001"}

	b, err := json.Marshal(args)
	require.Nil(t, err)
	body := bytes.NewBuffer(b)

	client := &http.Client{}
	req, err := http.NewRequest("DELETE", url, body)
	require.Nil(t, err)
	req.Header.Set("Content-Type", contentType)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("client.Do error,err:%s", err)
	}
	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	require.Nil(t, err)

	d := json.NewDecoder(bytes.NewReader(respBody))
	d.UseNumber()
	err = d.Decode(&httpResp)
	require.Nil(t, err)
	require.Equal(t, "success", httpResp.Message)
	members, ok := httpResp.Data.([]interface{})
	require.Equal(t, true, ok)

	require.Equal(t, 2, len(members))
	for i := 0; i < len(members); i++ {
		member, ok := members[i].(map[string]interface{})
		require.Equal(t, true, ok)
		require.NotEqual(t, args.NodeName, member["name"].(string))
		t.Logf("Id:%s,peerURLs:%s,name:%s", member["Id"], member["peerURLs"], member["name"])
	}
}

func TestAddMember(t *testing.T) {
	var httpResp utils.Resp
	cluster := StartClusterInTest()
	defer StopClusterInTest(cluster)

	//sleep 5 seconds,for cluster to be stable
	time.Sleep(time.Second * 5)
	url := fmt.Sprintf("http://%s/members", leader.adminSvr.AdminAddr)
	contentType := "application/json;charset=utf-8"

	args := struct {
		NodeName string `json:"name"`
		PeerURL  string `json:"peer_url"`
		AdminURL string `json:"admin_url"`
	}{NodeName: "node_4", PeerURL: "http://127.0.0.1:15009", AdminURL: "http://127.0.0.1:16008"}

	b, err := json.Marshal(args)
	require.Nil(t, err)
	body := bytes.NewBuffer(b)

	client := &http.Client{}
	req, err := http.NewRequest("POST", url, body)
	require.Nil(t, err)
	req.Header.Set("Content-Type", contentType)

	resp, err := client.Do(req)
	require.Nil(t, err)
	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	require.Nil(t, err)

	d := json.NewDecoder(bytes.NewReader(respBody))
	d.UseNumber()
	err = d.Decode(&httpResp)
	require.Nil(t, err)
	require.Equal(t, "success", httpResp.Message)
	members, ok := httpResp.Data.([]interface{})
	require.Equal(t, true, ok)

	require.Nil(t, err)
	require.Equal(t, 4, len(members))

	var exist = false
	for i := 0; i < len(members); i++ {
		member, ok := members[i].(map[string]interface{})
		require.Equal(t, true, ok)
		if member["name"].(string) == args.NodeName && member["peerURLs"].([]interface{})[0].(string) == args.PeerURL {
			exist = true
			break
		}
		t.Logf("Id:%s,peerURLs:%s,name:%s", member["Id"], member["peerURLs"], member["name"])
	}
	require.Equal(t, true, exist)
}
