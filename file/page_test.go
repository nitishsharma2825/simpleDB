package file

import "testing"

func TestWriteInt(t *testing.T) {
	page := NewPageWithSize(1024)

	const v = 77
	page.SetInt(0, 77)

	if got := page.GetInt(0); got != v {
		t.Fatalf("expected %d, got %d", v, got)
	}
}

func TestWritrIntLoop(t *testing.T) {
	page := NewPageWithSize(1024)

	nums := []int{256, 123, 1, 0, 1000000, 16543}

	j := 0
	for offset := 0; offset < len(nums)*4; offset += 4 {
		page.SetInt(offset, nums[j])
		j++
	}

	j = 0
	for offset := 0; offset < len(nums)*4; offset += 4 {
		v := page.GetInt(offset)
		if v != nums[j] {
			t.Fatalf("expected %d, got %d", nums[j], v)
		}
		j++
	}
}

func TestWriteString(t *testing.T) {
	page := NewPageWithSize(1024)

	const v = "this is a test"

	page.SetString(0, v)

	if got := page.GetString(0); got != v {
		t.Fatalf("expected %q, got %q", v, got)
	}
}

func TestWriteStringMultiple(t *testing.T) {
	page := NewPageWithSize(1024)

	const v = "this is a test"
	const v2 = "this is another test"

	page.SetString(0, v)

	off := MaxLength(len(v))
	page.SetString(off, v2)

	if got := page.GetString(0); got != v {
		t.Fatalf("expected %q, got %q", v, got)
	}

	if got := page.GetString(off); got != v2 {
		t.Fatalf("expected %q, got %q", v2, got)
	}
}
