package dsl_library

import (
	"fmt"
	"go.uber.org/cadence/workflow"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"
)

const (
	fieldSeparator = "."
	asterisk       = "*"
	startPayload   = "start-payload"
	packageName    = "main."
)

type (
	Payload map[string]any

	Workflow struct {
		Id          string    `json:"id,omitempty"`
		Description string    `json:"description,omitempty"`
		Trigger     []Trigger `json:"trigger,omitempty"`
		Root        Statement `json:"root"`
	}

	Trigger struct {
		TriggerType string `json:"triggerType"`
		EventType   string `json:"eventType"`
	}

	Statement struct {
		Step     *Step     `json:"step,omitempty"`
		Sequence *Sequence `json:"sequence,omitempty"`
		Parallel *Parallel `json:"parallel,omitempty"`
	}

	Sequence struct {
		Elements []*Statement `json:"elements,omitempty"`
	}

	Parallel struct {
		Branches []*Statement `json:"branches,omitempty"`
	}

	Step struct {
		StepType                       string `json:"stepType"`
		ScenarioId                     string `json:"scenarioId"`
		Webhook                        string `json:"webhook"`
		Method                         string `json:"method"`
		ScenarioCompletionNotification string `json:"scenarioCompletionNotification"`
		Input                          string `json:"input"`
		Output                         string `json:"output"`
	}

	Executable interface {
		Execute(ctx workflow.Context, p Payload) error
	}
)

func DSLWorkflow(ctx workflow.Context, dslWorkflow Workflow, p Payload) ([]byte, error) {
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		HeartbeatTimeout:       time.Second * 20,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	logger := workflow.GetLogger(ctx)

	binding := p
	err := dslWorkflow.Root.Execute(ctx, binding)
	if err != nil {
		logger.Error("DSL Workflow failed.", zap.Error(err))
		return nil, err
	}

	logger.Info("DSL Workflow completed.")
	return nil, err
}

func (b *Statement) Execute(ctx workflow.Context, binding Payload) error {
	if b.Parallel != nil {
		err := b.Parallel.Execute(ctx, binding)
		if err != nil {
			return err
		}
	}
	if b.Sequence != nil {
		err := b.Sequence.Execute(ctx, binding)
		if err != nil {
			return err
		}
	}
	if b.Step != nil {
		err := b.Step.Execute(ctx, binding)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a Step) Execute(ctx workflow.Context, binding Payload) error {
	var output Payload
	err := workflow.ExecuteActivity(ctx, packageName+a.ScenarioId, binding).Get(ctx, &output)
	binding[a.Output] = output
	if err != nil {
		return err
	}
	return nil
}

func (s Sequence) Execute(ctx workflow.Context, binding Payload) error {
	for _, a := range s.Elements {
		err := a.Execute(ctx, binding)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p Parallel) Execute(ctx workflow.Context, binding Payload) error {
	childCtx, cancelHandler := workflow.WithCancel(ctx)
	selector := workflow.NewSelector(ctx)
	var activityErr error
	for _, s := range p.Branches {
		f := executeAsync(s, childCtx, binding)
		selector.AddFuture(f, func(f workflow.Future) {
			err := f.Get(ctx, nil)
			if err != nil {
				cancelHandler()
				activityErr = err
			}
		})
	}

	for i := 0; i < len(p.Branches); i++ {
		selector.Select(ctx)
		if activityErr != nil {
			return activityErr
		}
	}
	return nil
}

func executeAsync(exe Executable, ctx workflow.Context, binding Payload) workflow.Future {
	future, settable := workflow.NewFuture(ctx)

	workflow.Go(ctx, func(ctx workflow.Context) {
		err := exe.Execute(ctx, binding)
		settable.Set(nil, err)
	})
	return future
}

func ProcessMap(p Payload, m Payload) (Payload, error) {
	if m == nil {
		return p, nil
	}

	payload := make(Payload)
	metadata := make(Payload)

	flattenMap(nil, p, payload)
	flattenMap(nil, m, metadata)

	o := make(map[string]any)
	for k, v := range payload {
		if meta, ok := metadata[k]; ok {
			if include, ok := meta.(bool); ok {
				if !include {
					continue
				} else {
					o[k] = v
				}
			}
		} else {
			idx := strings.LastIndex(k, fieldSeparator)
			if idx != -1 {
				search := fmt.Sprintf("%s.*", k[:idx])
				if _, ok := metadata[search]; ok {
					o[k] = v
				}
			} else {
				if _, ok := metadata[asterisk]; ok {
					if meta, ok := metadata[k]; ok {
						if include, ok := meta.(bool); ok {
							if !include {
								continue
							} else {
								o[k] = v
							}
						}
					} else {
						o[k] = v
					}
				}
			}
		}
	}
	return unflattenMap(o), nil
}

func flattenMap(prefix *string, m Payload, r Payload) {
	for k, v := range m {
		switch v.(type) {
		case Payload:
			x, _ := v.(Payload)
			key := getKeyName(prefix, k)
			flattenMap(&key, x, r)
		default:
			key := getKeyName(prefix, k)
			r[key] = v
		}
	}
}

func getKeyName(prefix *string, k string) string {
	if prefix != nil {
		return fmt.Sprintf("%s.%s", *prefix, k)
	}
	return k
}

func unflattenMap(m Payload) Payload {
	sk := getSortedKeys(m)
	res := make(Payload)

	for _, k := range sk {
		if isComposed(k) {
			im, lk := getMapEntry(res, k)
			im[lk] = m[k]
		} else {
			res[k] = m[k]
		}
	}
	return res
}

func isComposed(s string) bool {
	return strings.Contains(s, fieldSeparator)
}

func getSortedKeys(m Payload) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func getMapEntry(m Payload, k string) (Payload, string) {
	curMap := m
	var nk string
	for ; isComposed(k); k = k[strings.Index(k, fieldSeparator)+1:] {
		nk = k[:strings.Index(k, fieldSeparator)]
		if innerMap, ok := curMap[nk]; ok {
			curMap = innerMap.(Payload)
		} else {
			newMap := make(Payload)
			curMap[nk] = newMap
			curMap = newMap
		}
	}
	return curMap, k
}
