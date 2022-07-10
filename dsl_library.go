package dsl_library

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"strings"
	"time"
)

const (
	activityNamePrefix = "main.f_"
	referenceSign      = "$"
	fieldSeparator     = "."
	workflowId         = "workflowId"

	Eq    = "$eq"
	Neq   = "$neq"
	Lt    = "$lt"
	Lte   = "$lte"
	Gt    = "$gt"
	Gte   = "$gte"
	Start = "$start"
	End   = "$end"
)

type (
	Payload struct {
		Metadata map[string]any
		Ctx      map[string]any
	}

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
		Condition *Condition   `json:"condition,omitempty"`
		Elements  []*Statement `json:"elements,omitempty"`
	}

	Parallel struct {
		Condition *Condition   `json:"condition,omitempty"`
		Branches  []*Statement `json:"branches,omitempty"`
	}

	Step struct {
		StepType                       string                  `json:"stepType"`
		ScenarioId                     string                  `json:"scenarioId"`
		Condition                      *Condition              `json:"condition,omitempty"`
		Url                            string                  `json:"url"`
		Method                         string                  `json:"method"`
		Type                           string                  `json:"type,omitempty"`
		ScenarioCompletionNotification *CompletionNotification `json:"scenarioCompletionNotification,omitempty"`
		Input                          string                  `json:"input,omitempty"`
		Output                         string                  `json:"output,omitempty"`
	}

	Condition struct {
		Left  any    `json:"left"`
		Op    string `json:"op"`
		Right any    `json:"right"`
	}

	CompletionNotification struct {
		Event []string `json:"event,omitempty"`
	}

	Executable interface {
		Execute(ctx workflow.Context, p Payload) error
	}

	MakeRequest struct {
		Id          string         `json:"id"`
		Source      string         `json:"source"`
		SpecVersion string         `json:"specversion"`
		EventType   string         `json:"type"`
		ProcessId   string         `json:"processId"`
		Data        map[string]any `json:"data"`
	}

	Event struct {
		EventType string         `json:"eventType"`
		Body      map[string]any `json:"body"`
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

func (step Step) Execute(ctx workflow.Context, binding Payload) error {
	if checkCondition(step.Condition, step.ScenarioId, binding) {
		r := executeStep(ctx, binding, step)
		if step.ScenarioCompletionNotification != nil && len(step.ScenarioCompletionNotification.Event) > 0 {
			log.Infof("Waiting for event notification with event types: %s", step.ScenarioCompletionNotification.Event)
			waitForEvent(ctx, binding, step)
		}
		return r
	}
	return nil
}

func executeStep(ctx workflow.Context, binding Payload, step Step) error {
	var output Payload
	err := workflow.ExecuteActivity(ctx, activityNamePrefix+step.ScenarioId, binding).Get(ctx, &output)
	binding.Ctx[step.ScenarioId] = output.Ctx[step.ScenarioId]
	return err
}

func waitForEvent(ctx workflow.Context, binding Payload, step Step) {
	s := workflow.NewSelector(ctx)
	var r bool
	var eT string

	for !r {
		s.AddReceive(workflow.GetSignalChannel(ctx, binding.Metadata[workflowId].(string)), func(c workflow.Channel, ok bool) {
			var event Event
			ok1 := c.Receive(ctx, &event)
			eT = event.EventType

			if ok1 {
				if slices.Contains(step.ScenarioCompletionNotification.Event, event.EventType) {
					binding.Ctx[event.EventType] = event.Body
					r = true
				}
			}
		})
		s.Select(ctx)
	}
	log.Infof("Finishing waiting as event with type %s received.", eT)
}

func checkCondition(c *Condition, name string, binding Payload) bool {
	if c != nil {
		r, err := evaluateCondition(*c, binding)
		if err != nil {
			log.Errorf("Cannot process step. %s", err)
		} else if !r {
			log.Infof("Skipping step %s due to condition.", name)
			return false
		}
	}
	return true
}

func (s Sequence) Execute(ctx workflow.Context, binding Payload) error {
	if checkCondition(s.Condition, "*sequence*", binding) {
		for _, a := range s.Elements {
			err := a.Execute(ctx, binding)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p Parallel) Execute(ctx workflow.Context, binding Payload) error {
	if checkCondition(p.Condition, "*parallel*", binding) {
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

func getField(field any, binding Payload) (any, error) {
	switch field.(type) {
	case string:
		if strings.HasPrefix(field.(string), referenceSign) {
			return getFieldValue(field.(string)[1:], binding)
		} else {
			return field, nil
		}
	default:
		return field, nil
	}
}

func getFieldValue(f string, binding Payload) (any, error) {
	log.Debugf("Getting value of field %s", f)

	segments := strings.Split(f, fieldSeparator)
	size := len(segments)
	if size == 1 {
		return binding.Ctx[segments[0]], nil
	} else {
		cur := binding.Ctx
		var ok bool
		for i := 0; i < size-1; i++ {
			cur, ok = cur[segments[i]].(map[string]any)
			if !ok {
				return "", fmt.Errorf("cannot read field: %s", segments[i])
			}
		}
		return cur[segments[size-1]], nil
	}
}

func evaluateCondition(c Condition, binding Payload) (bool, error) {
	l, errL := getField(c.Left, binding)
	r, errR := getField(c.Right, binding)

	if errL != nil || errR != nil {
		return false, fmt.Errorf("cannot evaluate expression: %s %s", errL, errR)
	}

	switch c.Op {
	case Eq:
		return l == r, nil

	case Neq:
		return l != r, nil

	case Lt:
		lv, ok1 := l.(int)
		rv, ok2 := r.(int)

		if ok1 && ok2 {
			return lv < rv, nil
		} else {
			return false, fmt.Errorf("cannot convert %s", c)
		}

	case Lte:
		lv, ok1 := l.(int)
		rv, ok2 := r.(int)

		if ok1 && ok2 {
			return lv <= rv, nil
		} else {
			return false, fmt.Errorf("cannot convert %s", c)
		}

	case Gt:
		lv, ok1 := l.(int)
		rv, ok2 := r.(int)

		if ok1 && ok2 {
			return lv > rv, nil
		} else {
			return false, fmt.Errorf("cannot convert %s", c)
		}

	case Gte:
		lv, ok1 := l.(int)
		rv, ok2 := r.(int)

		if ok1 && ok2 {
			return lv >= rv, nil
		} else {
			return false, fmt.Errorf("cannot convert %s", c)
		}

	case Start:
		lv, ok1 := l.(string)
		rv, ok2 := r.(string)

		if ok1 && ok2 {
			return strings.HasPrefix(lv, rv), nil
		} else {
			return false, fmt.Errorf("cannot convert %s", c)
		}

	case End:
		lv, ok1 := l.(string)
		rv, ok2 := r.(string)

		if ok1 && ok2 {
			return strings.HasSuffix(lv, rv), nil
		} else {
			return false, fmt.Errorf("cannot convert %s", c)
		}

	default:
		return false, fmt.Errorf("unknown operator: %s", c.Op)
	}
}

func unmarshalEvent(s string) (Event, error) {
	var event Event

	err := json.Unmarshal([]byte(s), &event)
	if err != nil {
		log.Errorf("Cannot unmarshal event from signal. %s", err)
		return Event{}, err
	}
	return event, nil
}
