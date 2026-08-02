package protocol

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// 实现完 WriteMessage/ReadMessage 后运行: go test ./protocol/ -v

func TestRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	in := &Message{
		Header: Header{Codec: CodecJSON, Type: MsgRequest, Seq: 42},
		Body:   []byte(`{"method":"User.Get","args":[1]}`),
	}
	if err := WriteMessage(&buf, in); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	out, err := ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if out.Header.Seq != 42 || out.Header.Type != MsgRequest || out.Header.Codec != CodecJSON {
		t.Fatalf("header mismatch: %+v", out.Header)
	}
	if !bytes.Equal(out.Body, in.Body) {
		t.Fatalf("body mismatch: %q", out.Body)
	}
}

// 模拟粘包：两条消息在同一个流里背靠背，必须能依次读出且互不污染。
func TestStickyPackets(t *testing.T) {
	var buf bytes.Buffer
	for seq := uint64(1); seq <= 3; seq++ {
		msg := &Message{
			Header: Header{Type: MsgRequest, Seq: seq},
			Body:   bytes.Repeat([]byte{byte(seq)}, int(seq)*10),
		}
		if err := WriteMessage(&buf, msg); err != nil {
			t.Fatalf("WriteMessage seq=%d: %v", seq, err)
		}
	}
	for seq := uint64(1); seq <= 3; seq++ {
		out, err := ReadMessage(&buf)
		if err != nil {
			t.Fatalf("ReadMessage seq=%d: %v", seq, err)
		}
		if out.Header.Seq != seq || len(out.Body) != int(seq)*10 {
			t.Fatalf("seq=%d got header=%+v bodyLen=%d", seq, out.Header, len(out.Body))
		}
	}
}

// 模拟拆包：字节一个一个地到达（onebyte reader），ReadMessage 仍须读出完整消息。
func TestFragmentedPackets(t *testing.T) {
	var buf bytes.Buffer
	in := &Message{Header: Header{Type: MsgResponse, Seq: 7}, Body: []byte("hello")}
	if err := WriteMessage(&buf, in); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	out, err := ReadMessage(iotest{r: &buf})
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if out.Header.Seq != 7 || string(out.Body) != "hello" {
		t.Fatalf("got header=%+v body=%q", out.Header, out.Body)
	}
}

func TestBadMagic(t *testing.T) {
	raw := make([]byte, HeaderSize)
	raw[0], raw[1] = 0xde, 0xad // 错误的 magic
	_, err := ReadMessage(bytes.NewReader(raw))
	if !errors.Is(err, ErrBadMagic) {
		t.Fatalf("want ErrBadMagic, got %v", err)
	}
}

func TestBodyTooLong(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMessage(&buf, &Message{Header: Header{Seq: 1}}); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	raw := buf.Bytes()
	// 篡改 bodyLen 字段（头部最后 4 字节）为 0xFFFFFFFF
	for i := HeaderSize - 4; i < HeaderSize; i++ {
		raw[i] = 0xff
	}
	_, err := ReadMessage(bytes.NewReader(raw))
	if !errors.Is(err, ErrBodyTooLong) {
		t.Fatalf("want ErrBodyTooLong, got %v", err)
	}
}

// iotest 每次 Read 最多返回 1 字节，用来模拟极端拆包。
type iotest struct{ r io.Reader }

func (o iotest) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return o.r.Read(p)
}
