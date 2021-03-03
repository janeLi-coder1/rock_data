package main

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"time"
)

type SongRecord struct {
	SongRaw     string    `db:"song_raw"`
	SongClean   string    `db:"song_clean"`
	ArtistRaw   string    `db:"artist_raw"`
	ArtistClean string    `db:"artist_clean"`
	CallSign    string    `db:"call_sign"`
	Time        time.Time `db:"time"`
	UniqueId    string    `db:"unique_id"`
	Combined    string    `db:"combined"`
	IsFirst     bool      `db:"is_first"`
}

type ConnCtx struct {
	Conn     *sqlx.DB
	Database *Database
}

func getPgDsn(database *Database) string {
	return fmt.Sprintf("host=%s port=%d user=%s dbname=%s password=%s sslmode=disable",
		database.Ip, database.Port, database.User, database.Db, database.Password)
}

func getConn(database *Database) (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", getPgDsn(database))
	return db, err
}

func NewConn(database *Database) (*ConnCtx, error) {
	conn, err := getConn(database)
	if err != nil {
		return nil, err
	}
	return &ConnCtx{
		Conn:     conn,
		Database: database,
	}, nil
}

func CleanTable(database *Database) error {
	conn, err := NewConn(database)
	if err != nil {
		return err
	}

	return conn.cleanTable()
}

func (c *ConnCtx) cleanTable() error {
	res, err := c.Conn.Exec(fmt.Sprintf("DELETE FROM %s.%s", c.Database.Schema, c.Database.Table))
	if err != nil {
		return fmt.Errorf("clean table error: %s", err)
	}

	rowsAffected, _ := res.RowsAffected()
	InfoF("clean table and delete %d rows", rowsAffected)
	return nil
}

func (c *ConnCtx) BatchInsertRecords(records []*SongRecord) error {
	start := 0
	end := 0
	length := len(records)

	for start < length {
		var currentBatch []*SongRecord

		// limit each multi-insert row numbers
		if length-start <= c.Database.MaxMultiInsertNumber {
			currentBatch = records[start:]
			start = length
		} else {
			end += c.Database.MaxMultiInsertNumber
			currentBatch = records[start:end]
			start = end
		}

		DebugF("batch insert %d rows", len(currentBatch))
		template := c.getInsertQueryTempalte()
		res, err := c.Conn.NamedExec(template, currentBatch)
		if err != nil {
			WarningF(fmt.Sprintf("batch execute sql error: %s, try to insert row by row", err.Error()))
			err = c.InsertRecordsRowByRow(records)
			if err != nil {
				ErrorF(fmt.Sprintf("insert row by row error: %s", err))
			}
			return nil
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			ErrorF(fmt.Sprintf("pg server executes error: %s", err.Error()))
			continue
		}
		if rowsAffected != int64(len(currentBatch)) {
			WarningF(fmt.Sprintf("insert record affected row error: %d, suppose: %d", rowsAffected, len(currentBatch)))
		}
		DebugF("batch insert done: %d rows", len(currentBatch))
	}

	return nil
}

func (c *ConnCtx) InsertRecordsRowByRow(records []*SongRecord) error {
	template := c.getInsertQueryTempalte()
	stmt, err := c.Conn.PrepareNamed(template)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, item := range records {
		result, err := stmt.Exec(&item)
		if err != nil {
			ErrorF(fmt.Sprintf("execute sql error: %s", err.Error()))
			continue
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			ErrorF(fmt.Sprintf("pg server executes error: %s", err.Error()))
			continue
		}
		if rowsAffected != 1 {
			WarningF(fmt.Sprintf("insert record affected row error: %d", rowsAffected))
		}
		DebugF("insert done: %#v", item)
	}

	return nil
}

func (c *ConnCtx) getInsertQueryTempalte() string {
	template := fmt.Sprintf("INSERT INTO %s.%s (song_raw, song_clean, artist_raw, artist_clean, callsign, time, "+
		"unique_id, combined, is_first) VALUES (:song_raw, :song_clean, :artist_raw, :artist_clean, :call_sign, :time, "+
		":unique_id, :combined, :is_first)", c.Database.Schema, c.Database.Table)
	return template
}
