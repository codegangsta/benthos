package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/redpanda-data/benthos/v4/public/service"

	// Import all plugins defined within the repo.
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure/extended"

	v8 "rogchap.com/v8go"
)

func createV8Context() *v8.Context {
	iso := v8.NewIsolate()
	global := v8.NewObjectTemplate(iso)

	// TODO: Throw real exceptions
	workflow := v8.NewFunctionTemplate(iso, func(info *v8.FunctionCallbackInfo) *v8.Value {
		args := info.Args()
		if len(args) != 1 {
			return v8.Undefined(iso)
		}

		config, err := parseObject(info.Context(), args[0])
		if err != nil {
			log.Println(err)
			return v8.Undefined(iso)
		}
		log.Println("config", config)

		builder := service.NewStreamBuilder()
		err = builder.SetConfig(config)
		if err != nil {
			log.Fatalln(err)
		}

		stream, err := builder.Build()
		if err != nil {
			log.Fatalln(err)
		}

		err = stream.Run(context.Background())
		if err != nil {
			log.Fatalln(err)
		}

		return v8.Undefined(iso)
	})
	global.Set("runWorkflow", workflow)
	ctx := v8.NewContext(iso, global)

	return ctx
}

func parseObject(ctx *v8.Context, val *v8.Value) (interface{}, error) {
	object, _ := ctx.Global().Get("Object")

	if val.IsObject() {
		obj, _ := val.AsObject()

		propsRaw, _ := object.Object().MethodCall("keys", obj)
		propsObj, _ := propsRaw.AsObject()
		lengthRaw, _ := propsObj.Get("length")
		length := lengthRaw.Uint32()
		props := make([]string, length)
		for i := uint32(0); i < length; i++ {
			prop, err := propsObj.GetIdx(i)
			if err != nil {
				return nil, err
			}
			props[i] = prop.String()
		}

		// props, _ := obj.GetPropertyNames()
		result := make(map[string]interface{})

		for _, prop := range props {
			propVal, _ := obj.Get(prop)
			parsedPropVal, err := parseObject(ctx, propVal)
			if err != nil {
				return nil, err
			}
			result[prop] = parsedPropVal
		}

		return result, nil
	} else if val.IsArray() {
		arr, _ := val.AsObject()
		lengthRaw, _ := arr.Get("length")
		length := lengthRaw.Uint32()

		result := make([]interface{}, length)

		for i := uint32(0); i < length; i++ {
			val, _ := arr.GetIdx(i)
			parsedVal, err := parseObject(ctx, val)
			if err != nil {
				return nil, err
			}
			result[i] = parsedVal
		}

		return result, nil
	}

	// Parse simple types
	if val.IsBoolean() {
		return val.Boolean(), nil
	}
	if val.IsNumber() {
		return val.Number(), nil
	}
	if val.IsString() {
		return val.String(), nil
	}
	if val.IsNull() {
		return nil, nil
	}
	if val.IsUndefined() {
		return nil, nil
	}

	// We don't know how to parse this value. It could be a function, error, etc.
	return nil, fmt.Errorf("don't know how to handle value: %v", val)
}

func main() {
	log.Println("Loading example.js")
	jsdata, err := os.ReadFile("example.js")
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Creating V8 context")
	ctx := createV8Context()

	log.Println("Running example.js")
	_, err = ctx.RunScript(string(jsdata), "example.js")
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Done running")

	builder := service.NewStreamBuilder()

	data, err := os.ReadFile("example.json")
	if err != nil {
		log.Fatalln(err)
	}

	config := make(map[string]any)

	err = json.Unmarshal(data, &config)
	if err != nil {
		log.Fatalln(err)
	}

	err = builder.SetConfig(config)
	if err != nil {
		log.Fatalln(err)
	}

	stream, err := builder.Build()
	if err != nil {
		log.Fatalln(err)
	}

	err = stream.Run(context.Background())
	if err != nil {
		log.Fatalln(err)
	}
}
