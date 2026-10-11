package attrib

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

func TestBlockModeReadsCopyRowsAndRefusesAtCopyDone(t *testing.T) {
	// Rows that all name one tenant pass; the statement is not refused up front.
	h := copyHarnessWith(StrictOptions{Mode: StrictBlock})
	if rej := h.gateQuery(`COPY orders (id, tenant_id) FROM STDIN`); rej != nil {
		t.Fatalf("the statement waits for its rows: %+v", rej)
	}
	if rej := h.gate(&pgproto3.CopyData{Data: []byte("1\tacme\n2\tacme\n")}); rej != nil {
		t.Fatalf("a chunk is never refused: %+v", rej)
	}
	if rej := h.gate(&pgproto3.CopyDone{}); rej != nil {
		t.Fatalf("one tenant: %+v", rej)
	}
	if e := h.last(t); e.Source != SourceExplicit || e.Blocked || e.Violation != "" {
		t.Errorf("clean load: %+v", e)
	}

	// A row without a tenant makes the load unattributable: refused at CopyDone,
	// by aborting the COPY rather than answering the client.
	h = copyHarnessWith(StrictOptions{Mode: StrictBlock})
	h.gateQuery(`COPY orders (id, tenant_id) FROM STDIN`)
	h.gate(&pgproto3.CopyData{Data: []byte("1\tacme\n2\t\\N\n")})
	rej := h.gate(&pgproto3.CopyDone{})
	if rej == nil || !rej.AbortCopy || rej.Code != "42501" {
		t.Fatalf("an unattributable load must abort the copy: %+v", rej)
	}
	if e := h.last(t); !e.Blocked || e.Violation != "unknown" {
		t.Errorf("event: %+v", e)
	}

	// Binary cannot be read, so it is refused before any data is sent.
	h = copyHarnessWith(StrictOptions{Mode: StrictBlock})
	if rej := h.gateQuery(`COPY orders (id, tenant_id) FROM STDIN WITH (FORMAT binary)`); rej == nil || rej.AbortCopy {
		t.Errorf("binary should be refused outright: %+v", rej)
	}

	// An exempt role loads whatever it likes.
	h = copyHarnessWith(StrictOptions{Mode: StrictBlock, Exempt: []string{"app"}})
	h.gateQuery(`COPY orders (id, tenant_id) FROM STDIN`)
	h.gate(&pgproto3.CopyData{Data: []byte("1\t\\N\n")})
	if rej := h.gate(&pgproto3.CopyDone{}); rej != nil {
		t.Errorf("exempt: %+v", rej)
	}

	// The client giving up is never refused, and leaves nothing behind.
	h = copyHarnessWith(StrictOptions{Mode: StrictBlock})
	h.gateQuery(`COPY orders (id, tenant_id) FROM STDIN`)
	h.gate(&pgproto3.CopyData{Data: []byte("1\t\\N\n")})
	if rej := h.gate(&pgproto3.CopyFail{Message: "no"}); rej != nil {
		t.Errorf("CopyFail refused: %+v", rej)
	}
	if st := h.a.st(h.s); st.copyIn != nil {
		t.Error("the stream is still open after CopyFail")
	}
}

func TestRefusedQueryDoesNotLeaveACopyOpen(t *testing.T) {
	h := copyHarnessWith(StrictOptions{Mode: StrictBlock})
	// The COPY would be held, but a later statement in the same Query is refused,
	// so the database never sees the COPY.
	if rej := h.gateQuery(`COPY orders (id, tenant_id) FROM STDIN; SELECT * FROM orders WHERE id = 5`); rej == nil {
		t.Fatal("expected a rejection")
	}
	if st := h.a.st(h.s); st.copyIn != nil {
		t.Error("a COPY the database never received is still waiting for rows")
	}
}
