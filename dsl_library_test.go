package dsl_library

import (
	"github.com/franela/goblin"
	. "github.com/onsi/gomega"
	"testing"
)

func Test(t *testing.T) {
	g := goblin.Goblin(t)
	RegisterFailHandler(func(m string, _ ...int) { g.Fail(m) })

	g.Describe("getField", func() {
		g.It("should return simple string", func() {
			binding := Payload{}
			r, err := getField("test", binding)

			Expect(err).To(BeNil())
			Expect(r).To(Equal("test"))
		})

		g.It("should return int", func() {
			binding := Payload{}
			r, err := getField(15, binding)

			Expect(err).To(BeNil())
			Expect(r).To(Equal(15))
		})
	})

	g.Describe("getNestedFieldValue", func() {
		g.It("should return referenced string", func() {
			binding := Payload{Result: map[string]any{"test": "string"}}
			r, err := getField("$test", binding)

			Expect(err).To(BeNil())
			Expect(r).To(Equal("string"))
		})

		g.It("should return referenced string from nested object", func() {
			binding := Payload{Result: map[string]any{"test": map[string]any{"nested": "string"}}}
			r, err := getField("$test.nested", binding)

			Expect(err).To(BeNil())
			Expect(r).To(Equal("string"))
		})

		g.It("should return referenced string from multiple nested object", func() {
			binding := Payload{Result: map[string]any{"test": map[string]any{"nested": map[string]any{"more": "string"}}}}
			r, err := getField("$test.nested.more", binding)

			Expect(err).To(BeNil())
			Expect(r).To(Equal("string"))
		})

		g.It("should return referenced int from multiple nested object", func() {
			binding := Payload{Result: map[string]any{"test": map[string]any{"nested": map[string]any{"more": 11}}}}
			r, err := getField("$test.nested.more", binding)

			Expect(err).To(BeNil())
			Expect(r).To(Equal(11))
		})
	})

	g.Describe("evaluateCondition", func() {
		g.It("should evaluate boolean condition to true", func() {
			binding := Payload{Result: map[string]any{"11": map[string]any{"isTrue": true}}}
			r, err := evaluateCondition(Condition{
				Left:  "$11.isTrue",
				Op:    Eq,
				Right: true,
			}, binding)

			Expect(err).To(BeNil())
			Expect(r).To(BeTrue())
		})

		g.It("should evaluate boolean condition to false", func() {
			binding := Payload{Result: map[string]any{"11": map[string]any{"isTrue": true}}}
			r, err := evaluateCondition(Condition{
				Left:  "$11.isTrue",
				Op:    Eq,
				Right: false,
			}, binding)

			Expect(err).To(BeNil())
			Expect(r).To(BeFalse())
		})

		g.It("should return false of evaluation is not possible", func() {
			binding := Payload{Result: map[string]any{"11": true}}
			_, err := evaluateCondition(Condition{
				Left:  "$11.isTrue",
				Op:    Eq,
				Right: false,
			}, binding)

			Expect(err).To(Not(BeNil()))
			Expect(err.Error()).To(Not(BeEmpty()))
		})

		g.It("should compare strings", func() {
			binding := Payload{Result: map[string]any{"11": map[string]any{"foo": "bar"}}}
			r, err := evaluateCondition(Condition{
				Left:  "$11.foo",
				Op:    Eq,
				Right: "bar",
			}, binding)

			Expect(err).To(BeNil())
			Expect(r).To(BeTrue())
		})

		g.It("should evaluate eq integers", func() {
			binding := Payload{Result: map[string]any{"11": map[string]any{"int": 5}}}
			r, err := evaluateCondition(Condition{
				Left:  "$11.int",
				Op:    Eq,
				Right: 5,
			}, binding)

			Expect(err).To(BeNil())
			Expect(r).To(BeTrue())
		})

		g.It("should evaluate neq integers to false", func() {
			binding := Payload{Result: map[string]any{"11": map[string]any{"int": 5}}}
			r, err := evaluateCondition(Condition{
				Left:  "$11.int",
				Op:    Neq,
				Right: 6,
			}, binding)

			Expect(err).To(BeNil())
			Expect(r).To(BeTrue())
		})

		g.It("should evaluate lt integers", func() {
			binding := Payload{Result: map[string]any{"11": map[string]any{"int": 5}}}
			r, err := evaluateCondition(Condition{
				Left:  "$11.int",
				Op:    Lt,
				Right: 6,
			}, binding)

			Expect(err).To(BeNil())
			Expect(r).To(BeTrue())
		})

		g.It("should evaluate string start with", func() {
			binding := Payload{Result: map[string]any{"11": map[string]any{"string": "That is awesome"}}}
			r, err := evaluateCondition(Condition{
				Left:  "$11.string",
				Op:    Start,
				Right: "That",
			}, binding)

			Expect(err).To(BeNil())
			Expect(r).To(BeTrue())
		})

		g.It("should evaluate boolean true", func() {
			binding := Payload{Result: map[string]any{"11": map[string]any{"isTrue": true}}}
			r, err := evaluateCondition(Condition{
				Left:  "$11.isTrue",
				Op:    Eq,
				Right: true,
			}, binding)

			Expect(err).To(BeNil())
			Expect(r).To(BeTrue())
		})

		g.It("should evaluate boolean not true", func() {
			binding := Payload{Result: map[string]any{"11": map[string]any{"isTrue": true}}}
			r, err := evaluateCondition(Condition{
				Left:  "$11.isTrue",
				Op:    Neq,
				Right: true,
			}, binding)

			Expect(err).To(BeNil())
			Expect(r).To(BeFalse())
		})
	})
}
