package main

import (
	"fmt"
	"strconv"
	"time"
)

// if read goroutine done, check channel length and close it
func WriteRecordToDbThroughChannel(ch chan []string, database *Database) {
	wg.Add(1)
	DebugF("start a new write goroutine...")
	defer func() {
		wg.Done()
		DebugF("all done for a write goroutine...")
	}()

	connCtx, err := NewConn(database)
	if err != nil {
		// one more try
		connCtx, err = NewConn(database)
		if err != nil {
			ErrorF(err.Error())
			return
		}
	}

	for {
		select {
		case <-DoneSignal:
			if len(ch) == 0 {
				close(ch)
				return
			} else {
				DoneSignal <- true
				insertRowsToDbThroughChannel(ch, connCtx)
			}
		default:
			insertRowsToDbThroughChannel(ch, connCtx)
		}
	}
}

// insert current rows in channel to db
func insertRowsToDbThroughChannel(ch chan []string, connCtx *ConnCtx) {
	if len(ch) == 0 {
		return
	}

	var rawText [][]string
	for length := len(ch); length > 0; length-- {
		rawText = append(rawText, <-ch)
	}
	DebugF("get %d rows to batch process", len(rawText))

	songRecordRows, err := BatchFormatSongRecordRows(rawText)
	if err != nil {
		ErrorF("%s for row: %#v", err, rawText)
	}
	err = connCtx.BatchInsertRecords(songRecordRows)
	if err != nil {
		ErrorF(err.Error())
	}
}

// format each row to array
func BatchFormatSongRecordRows(text [][]string) ([]*SongRecord, error) {
	var ret []*SongRecord
	for _, item := range text {
		record, err := FormatSongRecordRow(item)
		if err != nil {
			ErrorF(err.Error())
			continue
		}
		ret = append(ret, record)
	}
	return ret, nil
}

// format one row to struct
func FormatSongRecordRow(row []string) (*SongRecord, error) {
	if len(row) != 9 {
		return nil, fmt.Errorf("invalid row: %#v", row)
	}

	timestamp, err := strconv.ParseInt(row[5], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("format record error, invalid timestamp: %s", row)
	}

	first, err := strconv.ParseBool(row[8])
	if err != nil {
		return nil, fmt.Errorf("format record error, invalid first sign: %s", row)
	}

	songRaw := row[0]
	songClean := row[1]
	artistRaw := row[2]
	artistClean := row[3]
	callSign := row[4]
	uniqueId := row[6]
	combined := row[7]

	if len(songRaw) == 0 || len(songClean) == 0 || len(artistRaw) == 0 || len(artistClean) == 0 || len(callSign) == 0 || len(combined) == 0 {
		WarningF("invalid record: %s", row)
	}
	if len(uniqueId) == 0 {
		return nil, fmt.Errorf("empty uniquedId for record: %s", row)

	}

	record := &SongRecord{
		SongRaw:     songRaw,
		SongClean:   songClean,
		ArtistRaw:   artistRaw,
		ArtistClean: artistClean,
		CallSign:    callSign,
		Time:        time.Unix(timestamp, 0),
		UniqueId:    uniqueId,
		Combined:    combined,
		IsFirst:     first,
	}
	return record, nil
}
