package main

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/anishathalye/porcupine"
)

type historyInput struct {
	Group    int    `json:"group"`
	Kind     string `json:"kind"`
	Value    string `json:"value,omitempty"`
	Expected string `json:"expected,omitempty"`
}

type historyOutput struct {
	Uncertain bool      `json:"uncertain,omitempty"`
	Value     string    `json:"value,omitempty"`
	Pair      [2]string `json:"pair"`
	Succeeded bool      `json:"succeeded,omitempty"`
}

type historyOperation struct {
	Client   int           `json:"client"`
	Call     int64         `json:"call_ns"`
	Return   int64         `json:"return_ns"`
	Input    historyInput  `json:"input"`
	Output   historyOutput `json:"output"`
	Revision int64         `json:"revision"`
	Error    string        `json:"error,omitempty"`
}

type historyState struct {
	Register string
	Exists   bool
	Pair     [2]string
	List     string
	Set      string
}

func historyModel() porcupine.Model {
	model := porcupine.NondeterministicModel{
		Init:  func() []any { return []any{historyState{}} },
		Equal: func(left, right any) bool { return left.(historyState) == right.(historyState) },
		Partition: func(operations []porcupine.Operation) [][]porcupine.Operation {
			groups := make(map[int][]porcupine.Operation)
			for _, operation := range operations {
				group := operation.Input.(historyInput).Group
				groups[group] = append(groups[group], operation)
			}
			keys := make([]int, 0, len(groups))
			for group := range groups {
				keys = append(keys, group)
			}
			slices.Sort(keys)
			partitions := make([][]porcupine.Operation, 0, len(keys))
			for _, group := range keys {
				partitions = append(partitions, groups[group])
			}
			return partitions
		},
		Step: func(current, request, response any) []any {
			state, input, output := current.(historyState), request.(historyInput), response.(historyOutput)
			next := state
			valid := true
			switch input.Kind {
			case "write":
				next.Register = input.Value
				next.Exists = true
			case "read":
				valid = output.Value == state.Register
			case "cas":
				applied := state.Exists && state.Register == input.Expected
				valid = output.Succeeded == applied
				if applied {
					next.Register = input.Value
					next.Exists = true
				}
			case "txn-write":
				next.Pair = [2]string{input.Value, input.Value}
			case "txn-read":
				valid = output.Pair == state.Pair
			case "append":
				var values []string
				if state.List != "" && json.Unmarshal([]byte(state.List), &values) != nil {
					return nil
				}
				if slices.Contains(values, input.Value) {
					return nil
				}
				values = append(values, input.Value)
				encoded, err := json.Marshal(values)
				if err != nil {
					return nil
				}
				next.List = string(encoded)
				valid = output.Value == next.List
			case "list-read":
				valid = output.Value == state.List
			case "set-add", "set-remove":
				var values []string
				if state.Set != "" && json.Unmarshal([]byte(state.Set), &values) != nil {
					return nil
				}
				index := slices.Index(values, input.Value)
				if input.Kind == "set-remove" {
					valid = output.Succeeded == (index >= 0)
				}
				if input.Kind == "set-add" && index < 0 {
					values = append(values, input.Value)
				}
				if input.Kind == "set-remove" && index >= 0 {
					values = slices.Delete(values, index, index+1)
				}
				slices.Sort(values)
				if len(values) == 0 {
					next.Set = ""
				} else {
					encoded, err := json.Marshal(values)
					if err != nil {
						return nil
					}
					next.Set = string(encoded)
				}
			case "set-read":
				valid = output.Value == state.Set
			default:
				return nil
			}
			if output.Uncertain {
				return []any{state, next}
			}
			if !valid {
				return nil
			}
			return []any{next}
		},
	}
	return model.ToModel()
}

func checkHistory(history []historyOperation, timeout time.Duration) (porcupine.CheckResult, error) {
	if len(history) == 0 || len(history) > historyBatchSize || timeout <= 0 || timeout > 30*time.Second {
		return porcupine.Unknown, fmt.Errorf("history/check deadline must be nonempty and bounded")
	}
	operations := make([]porcupine.Operation, len(history))
	clients := make(map[int][]historyOperation)
	for index, operation := range history {
		if operation.Call < 0 || operation.Return <= operation.Call || operation.Return == math.MaxInt64 ||
			operation.Input.Group < 0 || operation.Input.Group >= 16 || operation.Client < 0 || operation.Client >= 32 ||
			len(operation.Input.Value) > 64<<10 || len(operation.Input.Expected) > 64<<10 || len(operation.Output.Value) > 64<<10 {
			return porcupine.Illegal, fmt.Errorf("invalid history bounds at operation %d", index)
		}
		if operation.Output.Uncertain != (operation.Error != "") ||
			(operation.Output.Uncertain && operation.Revision != 0) || (!operation.Output.Uncertain && operation.Revision <= 0) {
			return porcupine.Illegal, fmt.Errorf("invalid history outcome at operation %d", index)
		}
		output := operation.Output
		if output.Uncertain && (output.Value != "" || output.Pair != [2]string{} || output.Succeeded) {
			return porcupine.Illegal, fmt.Errorf("uncertain history outcome has an acknowledged payload")
		}
		switch operation.Input.Kind {
		case "write", "txn-write", "set-add":
			if output.Value != "" || output.Pair != [2]string{} || output.Succeeded {
				return porcupine.Illegal, fmt.Errorf("invalid mutation response shape")
			}
		case "read", "list-read", "set-read", "append":
			if output.Pair != [2]string{} || output.Succeeded {
				return porcupine.Illegal, fmt.Errorf("invalid value response shape")
			}
		case "cas", "set-remove":
			if output.Value != "" || output.Pair != [2]string{} {
				return porcupine.Illegal, fmt.Errorf("invalid conditional response shape")
			}
		case "txn-read":
			if output.Value != "" || output.Succeeded {
				return porcupine.Illegal, fmt.Errorf("invalid transaction response shape")
			}
		default:
			return porcupine.Illegal, fmt.Errorf("unsupported history operation kind")
		}
		clients[operation.Client] = append(clients[operation.Client], operation)
		end := operation.Return
		if operation.Output.Uncertain {
			end = math.MaxInt64
		}
		operations[index] = porcupine.Operation{
			ClientId: operation.Client, Call: operation.Call, Return: end, Input: operation.Input, Output: operation.Output,
		}
	}
	for _, calls := range clients {
		slices.SortFunc(calls, func(left, right historyOperation) int {
			if left.Call < right.Call {
				return -1
			}
			if left.Call > right.Call {
				return 1
			}
			return 0
		})
		var revision int64
		for index, call := range calls {
			if index > 0 && call.Call < calls[index-1].Return {
				return porcupine.Illegal, fmt.Errorf("overlapping calls from one history client")
			}
			if !call.Output.Uncertain {
				if call.Revision < revision {
					return porcupine.Illegal, fmt.Errorf("history client observed a decreasing revision")
				}
				revision = call.Revision
			}
		}
	}
	for _, operation := range history {
		if operation.Output.Uncertain {
			continue
		}
		for _, previous := range history {
			if !previous.Output.Uncertain && previous.Return <= operation.Call && previous.Revision > operation.Revision {
				return porcupine.Illegal, fmt.Errorf("a completed earlier operation has a later revision")
			}
		}
	}
	result := porcupine.CheckOperationsTimeout(historyModel(), operations, timeout)
	if result != porcupine.Ok {
		return result, fmt.Errorf("external history check: %s", result)
	}
	return result, nil
}
