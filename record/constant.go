package record

import (
	"fmt"
	"hash/fnv"
)

type Constant struct {
	ival *int
	sval *string
}

func NewNilConstant() Constant {
	return Constant{
		ival: nil,
		sval: nil,
	}
}

func NewIntConstant(ival int) Constant {
	return Constant{ival: &ival}
}

func NewStringConstant(sval string) Constant {
	return Constant{sval: &sval}
}

func (c Constant) AsInt() int {
	if c.ival != nil {
		return *c.ival
	}
	return 0
}

func (c Constant) AsString() string {
	if c.sval != nil {
		return *c.sval
	}
	return ""
}

func (c Constant) Equals(other Constant) bool {
	if c.ival != nil && other.ival != nil {
		return *c.ival == *other.ival
	}
	if c.sval != nil && other.sval != nil {
		return *c.sval == *other.sval
	}
	return false
}

func (c Constant) ToString() string {
	if c.ival != nil {
		return fmt.Sprintf("%d", *c.ival)
	}
	return *c.sval
}

func (c Constant) HashCode() int {
	h := fnv.New32a()
	if c.ival != nil {
		h.Write([]byte(fmt.Sprintf("%d", *c.ival)))
	} else if c.sval != nil {
		h.Write([]byte(*c.sval))
	}
	return int(h.Sum32())
}

func (c Constant) CompareTo(other Constant) int {
	if c.ival != nil && other.ival != nil {
		if *c.ival < *other.ival {
			return -1
		} else if *c.ival > *other.ival {
			return 1
		} else {
			return 0
		}
	}

	if c.sval != nil && other.sval != nil {
		if *c.sval < *other.sval {
			return -1
		} else if *c.sval > *other.sval {
			return 1
		} else {
			return 0
		}
	}

	return 0
}
