package api

import (
	"net/http"
	"sync"

	"github.com/flike/kingbus/log"
	"github.com/flike/kingbus/utils"

	"github.com/flike/kingbus/config"

	"github.com/labstack/echo"
)

//BinlogServerHandler is used for the api call of handling binlog server
//handler with lock will keep calling api with no compete
type BinlogServerHandler struct {
	l   sync.Mutex
	svr Server
}

//StartBinlogServer implements start a binlog server
func (h *BinlogServerHandler) StartBinlogServer(echoCtx echo.Context) error {
	h.l.Lock()
	defer h.l.Unlock()

	var args config.BinlogServerConfig
	var err error

	defer func() {
		if err != nil {
			log.Log.Errorf("StartBinlogServer error,err: %s", err)
			echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
		}
	}()

	err = echoCtx.Bind(&args)
	if err != nil {
		return err
	}
	kingbusIP := h.svr.GetIP()
	//check args
	err = args.Check(kingbusIP)
	if err != nil {
		return err
	}
	//start syncer server
	err = h.svr.StartServer(config.BinlogServerType, &args)
	if err != nil {
		log.Log.Errorf("start server error,err:%s,args:%v", err, args)
		return err
	}

	return echoCtx.JSON(http.StatusOK, utils.NewResp().SetData(""))
}

//GetBinlogServerStatus implements get binlog server status in the runtime state
func (h *BinlogServerHandler) GetBinlogServerStatus(echoCtx echo.Context) error {
	h.l.Lock()
	defer h.l.Unlock()

	status := h.svr.GetServerStatus(config.BinlogServerType)
	if masterStatus, ok := status.(*config.BinlogServerStatus); ok {
		return echoCtx.JSON(http.StatusOK, utils.NewResp().SetData(masterStatus))
	}
	return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError("no resp"))
}

//StopBinlogServer implements stop binlog server
func (h *BinlogServerHandler) StopBinlogServer(echoCtx echo.Context) error {
	h.l.Lock()
	defer h.l.Unlock()
	h.svr.StopServer(config.BinlogServerType)
	return echoCtx.JSON(http.StatusOK, utils.NewResp().SetData(""))
}
