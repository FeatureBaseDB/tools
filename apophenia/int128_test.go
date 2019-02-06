package apophenia

import (
	"testing"
)

func Test_Int128Rotate(t *testing.T) {
	cases := []struct {
		in         Uint128
		bits       uint64
		outL, outR Uint128
	}{
		{in: Uint128{lo: 0x1}, bits: 1, outR: Uint128{lo: 0x0, hi: 1 << 63}, outL: Uint128{lo: 0x2, hi: 0}},
		{in: Uint128{lo: 0x11}, bits: 4, outR: Uint128{lo: 1, hi: 1 << 60}, outL: Uint128{lo: 0x110, hi: 0}},
		{in: Uint128{lo: 0x11}, bits: 65, outR: Uint128{lo: 1 << 63, hi: 8}, outL: Uint128{lo: 0, hi: 0x22}},
	}
	for _, c := range cases {
		u := c.in
		u.RotateRight(c.bits)
		if u != c.outR {
			t.Fatalf("rotate %s right by %d: expected %s, got %s",
				c.in, c.bits, c.outR, u)
		}
		u = c.in
		u.RotateLeft(c.bits)
		if u != c.outL {
			t.Fatalf("rotate %s left by %d: expected %s, got %s",
				c.in, c.bits, c.outL, u)
		}
	}
}

func Test_Int128Shift(t *testing.T) {
	cases := []struct {
		in         Uint128
		bits       uint64
		outL, outR Uint128
	}{
		{in: Uint128{lo: 0x1}, bits: 1, outR: Uint128{lo: 0x0, hi: 0}, outL: Uint128{lo: 0x2, hi: 0}},
		{in: Uint128{lo: 0x11}, bits: 4, outR: Uint128{lo: 1, hi: 0}, outL: Uint128{lo: 0x110, hi: 0}},
		{in: Uint128{lo: 0x11, hi: 0x3}, bits: 65, outR: Uint128{lo: 1, hi: 0}, outL: Uint128{lo: 0, hi: 0x22}},
		{in: Uint128{lo: 0, hi: 0x11}, bits: 68, outR: Uint128{lo: 1, hi: 0}, outL: Uint128{lo: 0, hi: 0}},
	}
	for _, c := range cases {
		u := c.in
		u.ShiftRight(c.bits)
		if u != c.outR {
			t.Fatalf("shift %s right by %d: expected %s, got %s",
				c.in, c.bits, c.outR, u)
		}
		u = c.in
		u.ShiftLeft(c.bits)
		if u != c.outL {
			t.Fatalf("shift %s left by %d: expected %s, got %s",
				c.in, c.bits, c.outL, u)
		}
	}
}
