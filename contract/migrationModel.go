package contract

import "time"

type MigrationModel struct {
	ID        uint      `json:"id"`
	Migration string    `json:"migration"`
	Batch     uint      `json:"batch"`
	CreatedAt time.Time `json:"created_at"`
}
