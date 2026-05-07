package harness

type completion struct {
	input  chan *unitOfWork
	output chan *unitOfWork
}

func newCompletion(input, output chan *unitOfWork) *completion {
	return &completion{
		input:  input,
		output: output,
	}
}

func (this *completion) Listen() {
	defer close(this.output)
	for unit := range this.input {
		for _, complete := range unit.completions {
			complete()
		}
		this.output <- unit
	}
}
