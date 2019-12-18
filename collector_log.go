package collector_log

import (
	"errors"
	"fmt"
	"github.com/go-pg/pg"
	"github.com/sirupsen/logrus"
	"runtime/debug"
	"sync"
	"time"
)

var (
	clOnce sync.Once
	cl     *CollectorLog
)

type CollectorLog struct {
	db             *pg.DB
	Log            *logrus.Entry
	writeLogEnable bool
	insertToDb     bool
}

type ExtraLog struct {
	Num        uint32
	ExecutedAt time.Time
}

type TaskLog struct {
	TableName         struct{}               `sql:"log"`
	Id                uint64                 `sql:"id,pk"`
	TaskName          string                 `sql:"task_name,notnull"`
	IsApiTask         bool                   `sql:"is_api_task,notnull"`
	TaskSource        string                 `sql:"task_source,notnull"`
	TaskDateFrom      string                 `sql:"task_date_from,notnull"`
	TaskDateTo        string                 `sql:"task_date_to,notnull"`
	TaskInitParams    map[string]interface{} `sql:"task_init_params"`
	TaskCollectParams map[string]interface{} `sql:"task_collect_params"`
	Errors            map[string]interface{} `sql:"errors"`
	Info              map[string]interface{} `sql:"info"`
	StartedAt         time.Time              `sql:"started_at,notnull"`
	FinishedAt        time.Time              `sql:"finished_at"`
	IsFinished        bool                   `sql:"is_finished,notnull"`
	ErrorMessage      string                 `sql:"error_message"`
	HasError          bool                   `sql:"has_error"`
	FromApiLoadedNum  int                    `sql:"from_api_loaded_num"`
	FromApiLoadedAt   time.Time              `sql:"from_api_loaded_at"`
	FromDbLoadedNum   int                    `sql:"from_db_loaded_num"`
	FromDbLoadedAt    time.Time              `sql:"from_db_loaded_at"`
	InsertedRowsNum   int                    `sql:"inserted_rows_num"`
	InsertedRowsAt    time.Time              `sql:"inserted_rows_at"`
	UpdatedRowsNum    int                    `sql:"updated_rows_num"`
	UpdatedRowsAt     time.Time              `sql:"updated_rows_at"`
	DeletedRowsNum    int                    `sql:"deleted_rows_num"`
	DeletedRowsAt     time.Time              `sql:"deleted_rows_at"`
	ExtraLogs         map[string]ExtraLog    `sql:"extra_logs"`
	ExtraInfo         string                 `sql:"extra_info"`
}

func CreateCollectorLog(log *logrus.Entry, db *pg.DB, insertToDb, writeLogEnable bool) *CollectorLog {
	clOnce.Do(func() {
		cl = &CollectorLog{Log: log, db: db, insertToDb: insertToDb, writeLogEnable: writeLogEnable}
	})
	return cl
}

func (cl *CollectorLog) CreateTableIfNeeded() {
	_, err := cl.db.Exec(`CREATE TABLE IF NOT EXISTS log (
		id                          BIGSERIAL PRIMARY KEY                  NOT NULL,
		task_name  		            TEXT 								   NOT NULL,
		is_api_task                 BOOLEAN								   NOT NULL,
		task_source                 TEXT 								   		   ,
		task_date_from              DATE 								           ,
		task_date_to                DATE 								           ,
		task_init_params            JSONB 								           ,
		task_collect_params         JSONB 								           ,
		started_at                  TIMESTAMP WITH TIME ZONE               NOT NULL,
		finished_at                 TIMESTAMP WITH TIME ZONE					   ,
		is_finished 				BOOLEAN  				 DEFAULT FALSE NOT NULL,
		error_message               TEXT										   ,
		errors                      JSONB                                          ,
		info                        JSONB                                          ,
		has_error                   BOOLEAN                  DEFAULT FALSE NOT NULL,
		from_api_loaded_num         INT											   ,
		from_api_loaded_at          TIMESTAMP WITH TIME ZONE					   ,
		from_db_loaded_num          INT											   ,
		from_db_loaded_at           TIMESTAMP WITH TIME ZONE					   ,
		inserted_rows_num           INT											   ,
		inserted_rows_at            TIMESTAMP WITH TIME ZONE					   ,
		updated_rows_num            INT						                       ,
		updated_rows_at             TIMESTAMP WITH TIME ZONE					   ,
		deleted_rows_num            INT											   ,
		deleted_rows_at             TIMESTAMP WITH TIME ZONE    				   ,
		extra_logs                  JSONB										   ,
		extra_info                  TEXT											
	)`)

	if err != nil {
		cl.Log.Panic(err)
	}
}

func (cl *CollectorLog) StartTaskLog(
	isApiTask bool,
	taskName string,
	source string,
	dateFrom, dateTo time.Time,
	otherParams map[string]interface{},
) *TaskLog {
	cl.Log.Infof("Start task: %s at %s", taskName, time.Now().String())

	return &TaskLog{
		IsApiTask:      isApiTask,
		TaskName:       taskName,
		TaskSource:     source,
		TaskDateFrom:   dateFrom.Format("2006-01-02"),
		TaskDateTo:     dateTo.Format("2006-01-02"),
		TaskInitParams: otherParams,
		StartedAt:      time.Now(),
	}
}

func (cl *CollectorLog) StockBulkNonCriticalErrorTask(taskLog *TaskLog, mm map[string]interface{}) {
	if len(mm) == 0 {
		cl.LogText("there are no other params")
		return
	}
	taskLog.HasError = true
	taskLog.Errors = mm
	if cl.writeLogEnable {
		cl.LogOtherParams("TaskNonCriticalErrors", mm)
	}
}

func (cl *CollectorLog) StockOtherParamsTask(taskLog *TaskLog, mm map[string]interface{}) {
	if len(mm) == 0 {
		cl.LogText("there are no other params")
		return
	}
	if taskLog.TaskInitParams == nil {
		taskLog.TaskInitParams = make(map[string]interface{}, len(mm))
	}
	for k, v := range mm {
		taskLog.TaskInitParams[k] = v
	}
	if cl.writeLogEnable {
		cl.LogOtherParams("TaskLogOtherParams", mm)
	}
}

func (cl *CollectorLog) StockBulkOtherParamsTask(taskLog *TaskLog, mm map[string]interface{}) {
	if len(mm) == 0 {
		cl.LogText("there are no other params")
		return
	}
	taskLog.TaskInitParams = mm
	if cl.writeLogEnable {
		cl.LogOtherParams("TaskOtherParams", mm)
	}
}

func (cl *CollectorLog) LogOtherParams(str string, mm map[string]interface{}) {
	cl.Log.Infof("%s: %#v\n", str, mm)
}

func (cl *CollectorLog) StockBulkCollectParamsTask(taskLog *TaskLog, mm map[string]interface{}) {
	if len(mm) == 0 {
		cl.LogText("there are no other params")
		return
	}
	taskLog.TaskCollectParams = mm
	if cl.writeLogEnable {
		cl.LogOtherParams("TaskCollectParams", mm)
	}
}

func (cl *CollectorLog) LogText(str string) {
	cl.Log.Info(str)
}

func (cl *CollectorLog) LogErrorWithField(whereError string, err error) {
	cl.Log.Errorf("An error %s occurred in %s\n", err, whereError)
}

func (cl *CollectorLog) SaveCriticalWithPanicErrorTask(taskLog *TaskLog, err error) {
	taskLog.ErrorMessage = err.Error()
	taskLog.HasError = true
	if cl.insertToDb {
		err := cl.db.Insert(taskLog)
		if err != nil {
			cl.Log.Panic(err)
		}
	}
	cl.Log.Panic(err)
}

func (cl *CollectorLog) SaveCriticalLogicErrorTask(taskLog *TaskLog, err error) {
	taskLog.ErrorMessage = err.Error()
	taskLog.HasError = true
	if cl.insertToDb {
		err := cl.db.Insert(taskLog)
		if err != nil {
			cl.Log.Panic(err)
		}
	}
	cl.LogError(err)
}

func (cl *CollectorLog) LogError(err error) {
	cl.Log.Errorf("An error %s occurred\n", err)
}

func (cl *CollectorLog) LogTaskLog(taskLog *TaskLog) {
	cl.Log.Infof("TaskLog: %#v\n", taskLog)
}

func (cl *CollectorLog) LogErrors(taskLog *TaskLog) {
	cl.Log.Infof("TaskLog Errors: %#v\n", taskLog.Errors)
}

func (cl *CollectorLog) SaveFinishedTaskLog(taskLog *TaskLog) {
	taskLog.IsFinished = true
	taskLog.FinishedAt = time.Now()
	err := cl.db.Insert(taskLog)
	if err != nil {
		cl.Log.Panic(err.Error())
	}
	cl.LogFinishTask(taskLog)
}

func (cl *CollectorLog) LogFinishTask(taskLog *TaskLog) {
	cl.Log.Infof("Task %s Finished\n", taskLog.TaskName)
}

func (cl *CollectorLog) StockLoadedFromApi(taskLog *TaskLog, loadedFromApiNum int) {
	taskLog.FromApiLoadedNum = loadedFromApiNum
	taskLog.FromApiLoadedAt = time.Now()
	if cl.writeLogEnable {
		cl.LogNumLoadedFromApi(loadedFromApiNum)
	}
}

func (cl *CollectorLog) LogNumLoadedFromApi(loadedFromApiNum int) {
	cl.Log.Infof("Loaded from API rows: %d\n", loadedFromApiNum)
}

func (cl *CollectorLog) StockLoadedFromDb(taskLog *TaskLog, loadedFromDbNum int) {
	taskLog.FromDbLoadedNum = loadedFromDbNum
	taskLog.FromDbLoadedAt = time.Now()
	if cl.writeLogEnable {
		cl.LogNumLoadedFromDb(loadedFromDbNum)
	}
}

func (cl *CollectorLog) LogNumLoadedFromDb(loadedFromDbNum int) {
	cl.Log.Infof("Loaded from DB rows: %d\n", loadedFromDbNum)
}

func (cl *CollectorLog) StockInsertedRows(
	taskLog *TaskLog,
	tableName string,
	insertedRowsNum int,
) {
	taskLog.InsertedRowsNum = insertedRowsNum
	taskLog.InsertedRowsAt = time.Now()
	if cl.writeLogEnable {
		cl.LogNumInsertedRows(tableName, insertedRowsNum)
	}
}

func (cl *CollectorLog) StockUpdatedRows(
	taskLog *TaskLog,
	tableName string,
	updatedRowsNum int,
) {
	taskLog.UpdatedRowsNum = updatedRowsNum
	taskLog.UpdatedRowsAt = time.Now()
	if cl.writeLogEnable {
		cl.LogNumUpdatedRows(tableName, updatedRowsNum)
	}
}

func (cl *CollectorLog) StockInsertedUpdatedRows(
	taskLog *TaskLog,
	tableName string,
	insertedRowsNum int,
	updatedRowsNum int,
) {
	taskLog.InsertedRowsNum = insertedRowsNum
	taskLog.InsertedRowsAt = time.Now()
	taskLog.UpdatedRowsNum = updatedRowsNum
	taskLog.UpdatedRowsAt = time.Now()
	if cl.writeLogEnable {
		cl.LogNumInsertedUpdatedRows(tableName, insertedRowsNum, updatedRowsNum)
	}
}

func (cl *CollectorLog) LogNumInsertedUpdatedRows(tableName string, insertedRowsNum, updatedRowsNum int) {
	if tableName != "" {
		cl.Log.Infof("Inserted/updated rows: %d/%d in table: %s\n", insertedRowsNum, updatedRowsNum, tableName)
	} else {
		cl.Log.Infof("Inserted/updated rows: %d/%d\n", insertedRowsNum, updatedRowsNum)
	}
}

func (cl *CollectorLog) LogNumInsertedRows(tableName string, insertedRowsNum int) {
	cl.Log.Infof("Inserted rows: %d in table %s\n", insertedRowsNum, tableName)
}

func (cl *CollectorLog) LogNumUpdatedRows(tableName string, updatedRowsNum int) {
	cl.Log.Infof("Updated rows: %d in table %s\n", updatedRowsNum, tableName)
}

func (cl *CollectorLog) StockDeletedRows(taskLog *TaskLog, deletedRowsNum int, tableName string) {
	taskLog.DeletedRowsNum = deletedRowsNum
	taskLog.DeletedRowsAt = time.Now()
	if cl.writeLogEnable {
		cl.LogNumDeletedRows(deletedRowsNum, tableName)
	}
}

func (cl *CollectorLog) LogNumDeletedRows(deletedRowsNum int, tableName string) {
	if tableName != "" {
		cl.Log.Infof("Deleted rows: %d/%d in table: %s\n", deletedRowsNum, tableName)
	} else {
		cl.Log.Infof("Deleted rows: %d/%d\n", deletedRowsNum)
	}
}

func (cl *CollectorLog) logExtra(taskLog *TaskLog, extraLogs map[string]ExtraLog) {
	for logName, extraLog := range extraLogs {
		taskLog.ExtraLogs[logName] = extraLog
	}
	err := cl.db.Update(taskLog)
	if err != nil {
		cl.Log.Panic(err)
	}
	if cl.writeLogEnable {
		for logName, extraLog := range extraLogs {
			cl.Log.Infof(logName+": %v", extraLog.Num)
		}
	}
}

func (cl *CollectorLog) Recover(taskLog *TaskLog) {
	if r := recover(); r != nil {
		var err error
		switch x := r.(type) {
		case string:
			err = errors.New(x)
		case error:
			err = x
		default:
			err = errors.New("unknown panic")
		}

		taskLog.ErrorMessage = fmt.Sprint(err)
		dbErr := cl.db.Update(taskLog)
		if dbErr != nil {
			cl.Log.Panic(dbErr)
		}
		cl.Log.Errorf("%s - %s", err, debug.Stack())
	}
}
