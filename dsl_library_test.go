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
}
