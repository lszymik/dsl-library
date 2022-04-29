package dsl_library

import (
	"go.uber.org/cadence/workflow"
	"time"

	"go.uber.org/zap"
)

type (
	Workflow struct {
		Trigger []Trigger
		Root    Statement
	}

	Trigger struct {
		TriggerType string `json:"triggerType"`
		EventType   string `json:"eventType"`
	}

	Statement struct {
		Step     *Step
		Sequence *Sequence
		Parallel *Parallel
	}

	Sequence struct {
		Elements []*Statement
	}

	Parallel struct {
		Branches []*Statement
	}

	Step struct {
		StepType                       string `json:"stepType"`
		ScenarioId                     string `json:"scenarioId"`
		Webhook                        string `json:"webhook"`
		ScenarioCompletionNotification string `json:"scenarioCompletionNotification"`
	}

	Executable interface {
		Execute(ctx workflow.Context) error
	}
)

func DSLWorkflow(ctx workflow.Context, dslWorkflow Workflow) ([]byte, error) {
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		HeartbeatTimeout:       time.Second * 20,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	logger := workflow.GetLogger(ctx)

	err := dslWorkflow.Root.Execute(ctx)
	if err != nil {
		logger.Error("DSL Workflow failed.", zap.Error(err))
		return nil, err
	}

	logger.Info("DSL Workflow completed.")
	return nil, err
}

func (b *Statement) Execute(ctx workflow.Context) error {
	if b.Parallel != nil {
		err := b.Parallel.Execute(ctx)
		if err != nil {
			return err
		}
	}
	if b.Sequence != nil {
		err := b.Sequence.Execute(ctx)
		if err != nil {
			return err
		}
	}
	if b.Step != nil {
		err := b.Step.Execute(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a Step) Execute(ctx workflow.Context) error {
	var result string
	err := workflow.ExecuteActivity(ctx, "main."+a.ScenarioId).Get(ctx, &result)
	if err != nil {
		return err
	}
	return nil
}

func (s Sequence) Execute(ctx workflow.Context) error {
	for _, a := range s.Elements {
		err := a.Execute(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p Parallel) Execute(ctx workflow.Context) error {
	childCtx, cancelHandler := workflow.WithCancel(ctx)
	selector := workflow.NewSelector(ctx)
	var activityErr error
	for _, s := range p.Branches {
		f := executeAsync(s, childCtx)
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

func executeAsync(exe Executable, ctx workflow.Context) workflow.Future {
	future, settable := workflow.NewFuture(ctx)

	workflow.Go(ctx, func(ctx workflow.Context) {
		err := exe.Execute(ctx)
		settable.Set(nil, err)
	})
	return future
}
