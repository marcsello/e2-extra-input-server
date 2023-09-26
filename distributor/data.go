package distributor

type Data struct {
	A float64 `json:"A"`
	B float64 `json:"B"`
	C float64 `json:"C"`
	D float64 `json:"D"`
	E float64 `json:"E"`
	F float64 `json:"F"`
}

type DataWithID struct {
	Data
	ID uint64 `json:"id"`
}
