package kafka

// InMemTopics stores the values, the offsets and the committed offsets of topics in memory
// structures.
type InMemTopics struct {
	topics  chan map[string]chan []int
	offsets chan map[string]chan int
}

// NewInMemTopics returns a new initialized InMemTopics.
func NewInMemTopics() InMemTopics {
	l := make(chan map[string]chan []int, 1)
	l <- map[string]chan []int{}
	o := make(chan map[string]chan int, 1)
	o <- map[string]chan int{}
	i := InMemTopics{
		topics:  l,
		offsets: o,
	}
	return i
}

// topic returns the channel of the topic with the specified name.
func (i InMemTopics) topic(name string) chan []int {
	logs := <-i.topics
	valuesLog, ok := logs[name]
	if !ok {
		valuesLog = make(chan []int, 1)
		valuesLog <- nil
		logs[name] = valuesLog
	}
	i.topics <- logs
	return valuesLog
}

// Send sends a message to a topic returning the corresponding offset.
func (i InMemTopics) Send(topics string, message int) int {
	logValues := i.topic(topics)
	values := <-logValues
	defer func() {
		logValues <- values
	}()
	values = append(values, message)
	offset := len(values) - 1
	return offset
}

// Poll returns a slice with the messages stored in the given topic, starting at
// a the given offset and finishing at the end of the log.
func (i InMemTopics) Poll(topic string, offset int) []int {
	log := i.topic(topic)
	values := <-log
	defer func() { log <- values }()
	if len(values)-1 < offset {
		return nil
	}
	return values[offset:]
}

// Commit commits an offset for the given topic If the offset to commit is less
// than the one already committed, the function does nothing.
func (i InMemTopics) Commit(topic string, offset int) {
	offsets := <-i.offsets
	keyOffset, ok := offsets[topic]
	if !ok {
		keyOffset = make(chan int, 1)
		keyOffset <- -1
		offsets[topic] = keyOffset
	}
	i.offsets <- offsets
	currentOffset := <-keyOffset
	defer func() {
		keyOffset <- currentOffset
	}()
	if offset <= currentOffset {
		return
	}
	currentOffset = offset
}

// Committed returns the current committed offset for a key. If no offset has
// been committed for the key, it returns -1.
func (i InMemTopics) Committed(topic string) int {
	offsets := <-i.offsets
	keyOffset, ok := offsets[topic]
	i.offsets <- offsets
	if !ok {
		return -1
	}
	offset := <-keyOffset
	keyOffset <- offset
	return offset
}
