package harness

import (
	"reflect"
	"strings"
)

func scan(types ...any) (executors map[reflect.Type][]executor, applicators map[reflect.Type][]applicator) {
	executors = make(map[reflect.Type][]executor)
	applicators = make(map[reflect.Type][]applicator)
	for _, v := range types {
		e, isExecutor := v.(executor)
		a, isApplicator := v.(applicator)
		if !isExecutor && !isApplicator {
			continue
		}
		t := reflect.TypeOf(v)
		for x := range t.NumMethod() {
			method := t.Method(x)
			argCount := method.Type.NumIn()
			if argCount <= 1 {
				continue
			}
			if method.Type.NumOut() > 0 {
				continue
			}
			if isExecutor && strings.HasPrefix(method.Name, "Execute") && len(method.Name) > len("Execute") {
				if argCount != 3 {
					continue
				}
				lastArg := method.Type.In(argCount - 1)
				if lastArg != reflect.TypeOf(func(...any) {}) {
					continue
				}
				handledType := method.Type.In(1)
				executors[handledType] = append(executors[handledType], e)
			}
			if isApplicator && strings.HasPrefix(method.Name, "Apply") && len(method.Name) > len("Apply") {
				if argCount != 2 {
					continue
				}
				handledType := method.Type.In(1)
				applicators[handledType] = append(applicators[handledType], a)
			}
		}
	}
	return executors, applicators
}
