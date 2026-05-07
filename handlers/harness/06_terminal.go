package harness

type terminal struct {
	input chan *unitOfWork
}

func newTerminal(input chan *unitOfWork) *terminal {
	return &terminal{input: input}
}

func (this *terminal) Listen() {
	for range this.input {
	}
}
