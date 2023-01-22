package cmd

import "encoding/json"

type UserInfo struct {
	UserId string
	Name   string
	Info   string
}

func (ui *UserInfo) GetId() []byte {
	return []byte(ui.UserId)
}

func (ui *UserInfo) Serialize() []byte {
	b, _ := json.Marshal(ui)
	return b
}

func (ui *UserInfo) Unserialize(b []byte) (err error) {
	return json.Unmarshal(b, ui)
}
