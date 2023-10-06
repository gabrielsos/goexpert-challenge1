package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

type ConversionResponse struct {
	USDBRL struct {
		Code       string `json:"code"`
		Codein     string `json:"codein"`
		Name       string `json:"name"`
		High       string `json:"high"`
		Low        string `json:"low"`
		VarBid     string `json:"varBid"`
		PctChange  string `json:"pctChange"`
		Bid        string `json:"bid"`
		Ask        string `json:"ask"`
		Timestamp  string `json:"timestamp"`
		CreateDate string `json:"create_date"`
	} `json:"USDBRL"`
}

type Conversion struct {
	ID         string    `db:"id"`
	Code       string    `db:"code"`
	Codein     string    `db:"codein"`
	Name       string    `db:"name"`
	High       string    `db:"high"`
	Low        string    `db:"low"`
	VarBid     string    `db:"var_bid"`
	PctChange  string    `db:"pct_change"`
	Bid        string    `db:"bid"`
	Ask        string    `db:"ask"`
	Timestamp  string    `db:"timestamp"`
	CreateDate string    `db:"create_date"`
	CreatedAt  time.Time `db:"created_at"`
	UpdatedAt  time.Time `db:"updated_at"`
	DeletedAt  time.Time `db:"deleted_at"`
}

func main() {
	db, err := sql.Open("sqlite3", "./sample.db")

	if err != nil {
		log.Fatalf("Erro ao conectar ao banco de dados: %s", err.Error())
	}

	defer db.Close()

	setupDatabase(db)

	http.HandleFunc("/cotacao", handleConversion)
	http.ListenAndServe(":8080", nil)
}

func setupDatabase(db *sql.DB) {
	tableCreationQuery := `CREATE TABLE IF NOT EXISTS conversions (
		"id"         VARCHAR PRIMARY KEY,
		code       VARCHAR,
		codein     VARCHAR,
		name       VARCHAR,
		high       VARCHAR,
		low        VARCHAR,
		var_bid     VARCHAR,
		pct_change  VARCHAR,
		bid        VARCHAR,
		ask        VARCHAR,
		timestamp  VARCHAR,
		create_date VARCHAR,
		created_at  DATETIME NOT NULL,
		updated_at  DATETIME NOT NULL,
		deleted_at  DATETIME  NULL
	)`
	_, err := db.Exec(tableCreationQuery)
	if err != nil {
		log.Fatalf("Erro ao criar a tabela: %s", err.Error())
	}
}

func handleConversion(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", "https://economia.awesomeapi.com.br/json/last/USD-BRL", nil)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	res, err := http.DefaultClient.Do(req)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	var conversion ConversionResponse

	err = json.Unmarshal(body, &conversion)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	db, err := sql.Open("sqlite3", "./sample.db")

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	defer db.Close()

	dbConversion := NewConversion(
		conversion.USDBRL.Code,
		conversion.USDBRL.Codein,
		conversion.USDBRL.Name,
		conversion.USDBRL.High,
		conversion.USDBRL.Low,
		conversion.USDBRL.VarBid,
		conversion.USDBRL.PctChange,
		conversion.USDBRL.Bid,
		conversion.USDBRL.Ask,
		conversion.USDBRL.Timestamp,
		conversion.USDBRL.CreateDate,
	)

	err = insertConversion(db, dbConversion)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

func NewConversion(
	code string,
	codein string,
	name string,
	high string,
	low string,
	varBid string,
	pctChange string,
	bid string,
	ask string,
	timestamp string,
	createDate string,
) *Conversion {
	return &Conversion{
		ID:         uuid.New().String(),
		Code:       code,
		Codein:     codein,
		Name:       name,
		High:       high,
		Low:        low,
		VarBid:     varBid,
		PctChange:  pctChange,
		Bid:        bid,
		Ask:        ask,
		Timestamp:  timestamp,
		CreateDate: createDate,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
		DeletedAt:  time.Now(),
	}
}

func insertConversion(db *sql.DB, conversion *Conversion) error {
	stmt, err := db.Prepare("insert into conversions(id, code, codein, name, high, low, var_bid, pct_change, bid, ask, timestamp, create_date, created_at, updated_at) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

	if err != nil {
		return err
	}

	defer stmt.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)

	defer cancel()

	_, err = stmt.ExecContext(
		ctx,
		conversion.ID,
		conversion.Code,
		conversion.Codein,
		conversion.Name,
		conversion.High,
		conversion.Low,
		conversion.VarBid,
		conversion.PctChange,
		conversion.Bid,
		conversion.Ask,
		conversion.Timestamp,
		conversion.CreateDate,
		conversion.CreatedAt,
		conversion.UpdatedAt,
	)

	if err != nil {
		return err
	}

	return nil
}
