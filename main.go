package main

import (
	"encoding/csv"
	"io"
	"os"
	"fmt"
	"github.com/faerulsalamun/go-csv-mongodb-wilayah-administrasi-indonesia/db"
	"log"
	"gopkg.in/mgo.v2/bson"
)

type Province struct {
	ID   bson.ObjectId `json:"_id" bson:"_id,omitempty"`
	Name string        `json:"name"`
}

type Regency struct {
	ID         bson.ObjectId `json:"_id" bson:"_id,omitempty"`
	ProvinceID bson.ObjectId `json:"province_id" bson:"province_id,omitempty"`
	Name       string        `json:"name"`
}

func main() {

	fmt.Println("Start insert data")

	con := db.Init()

	defer db.CloseSession()

	file, err := os.Open("data/provinces.csv")

	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	defer file.Close()

	reader := csv.NewReader(file)

	reader.Comma = ','

	for {
		record, err := reader.Read()

		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error:", err)
			return
		}

		var id_province = bson.NewObjectId()

		err = con.C("provincies").Insert(&Province{ID: id_province, Name: record[1]})

		if err != nil {
			log.Fatal(err)
		}

		// Search regencies
		file_regencies, err_regencies := os.Open("data/regencies.csv")

		if err_regencies != nil {
			fmt.Println("Error:", err)
			return
		}

		defer file_regencies.Close()

		reader_regencies := csv.NewReader(file_regencies)

		reader_regencies.Comma = ','

		var regencies []interface{}

		for {
			record_regencies, err_regencies := reader_regencies.Read()

			if err_regencies == io.EOF {
				break
			} else if err_regencies != nil {
				fmt.Println("Error:", err)
				return
			}

			if record[0] == record_regencies[1] {
				regencies = append(regencies, &Regency{
					ID:         bson.NewObjectId(),
					ProvinceID: id_province,
					Name:       record_regencies[2]})
			}

		}

		bulk := con.C("regencies").Bulk()
		bulk.Insert(regencies...)

		_, bulkErr := bulk.Run()

		if bulkErr != nil {
			panic(err)
		}

	}

	fmt.Println("Done insert data")
}
