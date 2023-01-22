package atur

type Object interface {
	GetId() []byte
	Serializable
}
