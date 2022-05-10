package io_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestHTTPClientRetries(t *testing.T) {
	var reqCount uint32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint32(&reqCount, 1)
		http.Error(w, "test error", http.StatusForbidden)
	}))
	defer ts.Close()

	conf := processor.NewConfig()
	conf.Type = "http"
	conf.HTTP.Config.URL = ts.URL + "/testpost"
	conf.HTTP.Config.Retry = "1ms"
	conf.HTTP.Config.NumRetries = 3

	h, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessMessage(message.QuickBatch([][]byte{[]byte("test")}))
	if res != nil {
		t.Fatal(res)
	}
	if len(msgs) != 1 {
		t.Fatal("Wrong count of error messages")
	}
	if msgs[0].Len() != 1 {
		t.Fatal("Wrong count of error message parts")
	}
	if exp, act := "test", string(msgs[0].Get(0).Get()); exp != act {
		t.Errorf("Wrong message contents: %v != %v", act, exp)
	}
	assert.Error(t, msgs[0].Get(0).ErrorGet())
	if exp, act := "403", msgs[0].Get(0).MetaGet("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	}

	if exp, act := uint32(4), atomic.LoadUint32(&reqCount); exp != act {
		t.Errorf("Wrong count of HTTP attempts: %v != %v", exp, act)
	}
}

func TestHTTPClientBasic(t *testing.T) {
	i := 0
	expPayloads := []string{"foo", "bar", "baz"}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqBytes, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if exp, act := expPayloads[i], string(reqBytes); exp != act {
			t.Errorf("Wrong payload value: %v != %v", act, exp)
		}
		i++
		w.Header().Add("foobar", "baz")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("foobar"))
	}))
	defer ts.Close()

	conf := processor.NewConfig()
	conf.Type = "http"
	conf.HTTP.Config.URL = ts.URL + "/testpost"

	h, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessMessage(message.QuickBatch([][]byte{[]byte("foo")}))
	if res != nil {
		t.Error(res)
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "201", msgs[0].Get(0).MetaGet("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	} else if exp, act := "", msgs[0].Get(0).MetaGet("foobar"); exp != act {
		t.Errorf("Wrong metadata value: %v != %v", act, exp)
	}

	msgs, res = h.ProcessMessage(message.QuickBatch([][]byte{[]byte("bar")}))
	if res != nil {
		t.Error(res)
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "201", msgs[0].Get(0).MetaGet("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	} else if exp, act := "", msgs[0].Get(0).MetaGet("foobar"); exp != act {
		t.Errorf("Wrong metadata value: %v != %v", act, exp)
	}

	// Check metadata persists.
	msg := message.QuickBatch([][]byte{[]byte("baz")})
	msg.Get(0).MetaSet("foo", "bar")
	msgs, res = h.ProcessMessage(msg)
	if res != nil {
		t.Error(res)
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "bar", msgs[0].Get(0).MetaGet("foo"); exp != act {
		t.Errorf("Metadata not preserved: %v != %v", act, exp)
	} else if exp, act := "201", msgs[0].Get(0).MetaGet("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	} else if exp, act := "", msgs[0].Get(0).MetaGet("foobar"); exp != act {
		t.Errorf("Wrong metadata value: %v != %v", act, exp)
	}
}

func TestHTTPClientEmptyResponse(t *testing.T) {
	i := 0
	expPayloads := []string{"foo", "bar", "baz"}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqBytes, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if exp, act := expPayloads[i], string(reqBytes); exp != act {
			t.Errorf("Wrong payload value: %v != %v", act, exp)
		}
		i++
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	conf := processor.NewConfig()
	conf.Type = "http"
	conf.HTTP.Config.URL = ts.URL + "/testpost"

	h, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessMessage(message.QuickBatch([][]byte{[]byte("foo")}))
	if res != nil {
		t.Error(res)
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "200", msgs[0].Get(0).MetaGet("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	}

	msgs, res = h.ProcessMessage(message.QuickBatch([][]byte{[]byte("bar")}))
	if res != nil {
		t.Error(res)
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "200", msgs[0].Get(0).MetaGet("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	}

	// Check metadata persists.
	msg := message.QuickBatch([][]byte{[]byte("baz")})
	msg.Get(0).MetaSet("foo", "bar")
	msgs, res = h.ProcessMessage(msg)
	if res != nil {
		t.Error(res)
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "200", msgs[0].Get(0).MetaGet("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	}
}

func TestHTTPClientEmpty404Response(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	conf := processor.NewConfig()
	conf.Type = "http"
	conf.HTTP.Config.URL = ts.URL + "/testpost"

	h, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessMessage(message.QuickBatch([][]byte{[]byte("foo")}))
	if res != nil {
		t.Error(res)
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foo", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "404", msgs[0].Get(0).MetaGet("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	} else {
		assert.Error(t, msgs[0].Get(0).ErrorGet())
	}
}

func TestHTTPClientBasicWithMetadata(t *testing.T) {
	i := 0
	expPayloads := []string{"foo", "bar", "baz"}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqBytes, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if exp, act := expPayloads[i], string(reqBytes); exp != act {
			t.Errorf("Wrong payload value: %v != %v", act, exp)
		}
		i++
		w.Header().Add("foobar", "baz")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("foobar"))
	}))
	defer ts.Close()

	conf := processor.NewConfig()
	conf.Type = "http"
	conf.HTTP.Config.URL = ts.URL + "/testpost"
	conf.HTTP.ExtractMetadata.IncludePatterns = []string{".*"}

	h, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessMessage(message.QuickBatch([][]byte{[]byte("foo")}))
	if res != nil {
		t.Error(res)
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "201", msgs[0].Get(0).MetaGet("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	} else if exp, act := "baz", msgs[0].Get(0).MetaGet("foobar"); exp != act {
		t.Errorf("Wrong metadata value: %v != %v", act, exp)
	}
}

func TestHTTPClientSerial(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		if string(bodyBytes) == "bar" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("foobar " + string(bodyBytes)))
	}))
	defer ts.Close()

	conf := processor.NewConfig()
	conf.Type = "http"
	conf.HTTP.Config.URL = ts.URL + "/testpost"

	h, err := mock.NewManager().NewProcessor(conf)
	require.NoError(t, err)

	inputMsg := message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
		[]byte("qux"),
		[]byte("quz"),
	})
	inputMsg.Get(0).MetaSet("foo", "bar")
	msgs, res := h.ProcessMessage(inputMsg)
	require.NoError(t, res)
	require.Len(t, msgs, 1)
	require.Equal(t, 5, msgs[0].Len())

	assert.Equal(t, "foobar foo", string(msgs[0].Get(0).Get()))
	assert.Equal(t, "bar", string(msgs[0].Get(1).Get()))
	require.Error(t, msgs[0].Get(1).ErrorGet())
	assert.Contains(t, msgs[0].Get(1).ErrorGet().Error(), "request returned unexpected response code")
	assert.Equal(t, "foobar baz", string(msgs[0].Get(2).Get()))
	assert.Equal(t, "foobar qux", string(msgs[0].Get(3).Get()))
	assert.Equal(t, "foobar quz", string(msgs[0].Get(4).Get()))
}

func TestHTTPClientParallel(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(5)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wg.Done()
		wg.Wait()
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("foobar"))
	}))
	defer ts.Close()

	conf := processor.NewConfig()
	conf.Type = "http"
	conf.HTTP.Config.URL = ts.URL + "/testpost"
	conf.HTTP.Parallel = true

	h, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	inputMsg := message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
		[]byte("qux"),
		[]byte("quz"),
	})
	inputMsg.Get(0).MetaSet("foo", "bar")
	msgs, res := h.ProcessMessage(inputMsg)
	if res != nil {
		t.Error(res)
	} else if expC, actC := 5, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "bar", msgs[0].Get(0).MetaGet("foo"); exp != act {
		t.Errorf("Metadata not preserved: %v != %v", act, exp)
	} else if exp, act := "201", msgs[0].Get(0).MetaGet("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	}
}

func TestHTTPClientParallelError(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(5)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wg.Done()
		wg.Wait()
		reqBytes, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(reqBytes) == "baz" {
			http.Error(w, "test error", http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte("foobar"))
	}))
	defer ts.Close()

	conf := processor.NewConfig()
	conf.Type = "http"
	conf.HTTP.Config.URL = ts.URL + "/testpost"
	conf.HTTP.Parallel = true
	conf.HTTP.Config.NumRetries = 0

	h, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessMessage(message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
		[]byte("qux"),
		[]byte("quz"),
	}))
	if res != nil {
		t.Error(res)
	}
	if expC, actC := 5, msgs[0].Len(); actC != expC {
		t.Fatalf("Wrong result count: %v != %v", actC, expC)
	}
	if exp, act := "baz", string(msgs[0].Get(2).Get()); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	assert.Error(t, msgs[0].Get(2).ErrorGet())
	if exp, act := "403", msgs[0].Get(2).MetaGet("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	}
	for _, i := range []int{0, 1, 3, 4} {
		if exp, act := "foobar", string(msgs[0].Get(i).Get()); act != exp {
			t.Errorf("Wrong result: %v != %v", act, exp)
		}
		assert.NoError(t, msgs[0].Get(i).ErrorGet())
		if exp, act := "200", msgs[0].Get(i).MetaGet("http_status_code"); exp != act {
			t.Errorf("Wrong response code metadata: %v != %v", act, exp)
		}
	}
}
