package dsl_library

import (
	"go.uber.org/cadence/workflow"
	"time"

	"go.uber.org/zap"
)

const (
	packageName = "main."
	resultName  = "-result"
)

type (
	Payload struct {
		Metadata map[string]any
		Data     map[string]any
		Result   map[string]any
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
		Condition Condition    `json:"condition,omitempty"`
		Elements  []*Statement `json:"elements,omitempty"`
	}

	Parallel struct {
		Condition Condition    `json:"condition,omitempty"`
		Branches  []*Statement `json:"branches,omitempty"`
	}

	Step struct {
		StepType                       string    `json:"stepType"`
		ScenarioId                     string    `json:"scenarioId"`
		Condition                      Condition `json:"condition,omitempty"`
		Url                            string    `json:"url"`
		Method                         string    `json:"method"`
		Type                           string    `json:"type,omitempty"`
		ScenarioCompletionNotification string    `json:"scenarioCompletionNotification,omitempty"`
		Input                          string    `json:"input,omitempty"`
		Output                         string    `json:"output,omitempty"`
	}

	Condition struct {
		Left  any    `json:"left"`
		Op    string `json:"op"`
		Right any    `json:"right"`
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
	binding.Data[a.ScenarioId+resultName] = output
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
