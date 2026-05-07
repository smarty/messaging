package harness

import "reflect"

// router dispatches messages to Execute*/Apply* methods discovered reflectively
// from the domain types registered via Options.Types(...). It is unexported
// because callers never hold one directly — build(ctx, cfg) constructs the sole
// instance per pipeline and feeds it into the execution stage as its executor
// collaborator. The router is not safe for concurrent use; the pipeline only
// calls Execute from within a single execution goroutine.
type router struct {
	executors   map[reflect.Type][]executor
	applicators map[reflect.Type][]applicator
	exclusions  []exclusion
}
type exclusion struct {
	message any
	self    applicator
}

func newRouter(types ...any) *router {
	executors, applicators := scan(types...)
	return &router{
		executors:   executors,
		applicators: applicators,
	}
}

func (this *router) Apply(message any) {
	for _, a := range this.applicators[reflect.TypeOf(message)] {
		a.Apply(message)
	}
}

func (this *router) Execute(message any, broadcast func(...any)) {
	for _, e := range this.executors[reflect.TypeOf(message)] {
		e.Execute(message, func(results ...any) {
			for _, result := range results {
				self := this.selfApplicator(result, e)
				if self != nil {
					self.Apply(result)
				}
				this.exclusions = append(this.exclusions, exclusion{message: result, self: self})
			}
			broadcast(results...)
		})
	}
	for _, e := range this.exclusions {
		for _, a := range this.applicators[reflect.TypeOf(e.message)] {
			if a == e.self {
				continue
			}
			a.Apply(e.message)
		}
	}
	this.exclusions = this.exclusions[:0]
}
func (this *router) selfApplicator(result any, e executor) applicator {
	for _, a := range this.applicators[reflect.TypeOf(result)] {
		if any(a) == any(e) {
			return a
		}
	}
	return nil
}
