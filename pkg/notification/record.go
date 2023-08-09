package notification

type NotificationRecord struct {
	Id        int64
	PromiseId string
	Url       string
	Time      int64
	Attempt   int64
}
