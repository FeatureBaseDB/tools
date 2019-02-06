package apophenia

import "fmt"

// Uint128 is an array of 2 uint64, treated as a single
// object to simplify calling conventions.
type Uint128 struct{ lo, hi uint64 }

// PadUint128 pads a 64-bit uint to 128 bits.
func PadUint128(u uint64) Uint128 {
	return Uint128{lo: u}
}

// MakeUint128 returns a Uint128 with low-order bits lo and high-order bits hi.
func MakeUint128(hi, lo uint64) Uint128 {
	return Uint128{lo: lo, hi: hi}
}

// Add adds value to its receiver in place.
func (u *Uint128) Add(value Uint128) {
	u.lo += value.lo
	if u.lo < value.lo {
		u.hi++
	}
	u.hi += value.hi
}

// Sub subtracts value from its receiver in place.
func (u *Uint128) Sub(value Uint128) {
	u.lo -= value.lo
	if u.lo > value.lo {
		u.hi--
	}
	u.hi -= value.hi
}

// And does a bitwise and with value, in place.
func (u *Uint128) And(value Uint128) {
	u.lo, u.hi = u.lo&value.lo, u.hi&value.hi
}

// Or does a bitwise or with value, in place.
func (u *Uint128) Or(value Uint128) {
	u.lo, u.hi = u.lo|value.lo, u.hi|value.hi
}

// Xor does a bitwise xor with value, in place.
func (u *Uint128) Xor(value Uint128) {
	u.lo, u.hi = u.lo^value.lo, u.hi^value.hi
}

// Not does a bitwise complement in place.
func (u *Uint128) Not() {
	u.lo, u.hi = ^u.lo, ^u.hi
}

// Low returns the low-order bits of u.
func (u *Uint128) Low() uint64 {
	return u.lo
}

// High returns the high-order bits of u.
func (u *Uint128) High() uint64 {
	return u.hi
}

// SetLow sets the low order bits of u.
func (u *Uint128) SetLow(lo uint64) {
	u.lo = lo
}

// SetHigh sets the high order bits of u.
func (u *Uint128) SetHigh(hi uint64) {
	u.hi = hi
}

// Mask produces a mask of the lower n bits of u.
func (u *Uint128) Mask(n uint64) {
	if n >= 128 {
		return
	}
	if n >= 64 {
		u.hi &= (1 << (n & 63)) - 1
	} else {
		u.lo &= (1 << n) - 1
	}
}

// Mask produces a bitmask with n bits set.
func Mask(n uint64) (u Uint128) {
	if n >= 128 {
		u.Not()
		return u
	}
	if n >= 64 {
		u.lo--
		u.hi = (1 << n & 63) - 1
		return u
	}
	u.lo = (1 << n) - 1
	return u
}

// String provides a string representation.
func (u Uint128) String() string {
	return fmt.Sprintf("0x%x%016x", u.hi, u.lo)
}

// bit rotation: for 1-63 bits, we are moving the low-order N bits of u.lo
// into the high-order N bits of u.hi, and vice versa. For 64-127, it's that
// plus swapping u.lo and u.hi.

// RotateRight rotates u right by n bits.
func (u *Uint128) RotateRight(n uint64) {
	if n&64 != 0 {
		u.lo, u.hi = u.hi, u.lo
	}
	n &= 63
	if n == 0 {
		return
	}
	unbits := 64 - n

	u.lo, u.hi = (u.lo>>n)|(u.hi<<unbits), (u.hi>>n)|(u.lo<<unbits)
}

// RotateLeft rotates u left by n bits.
func (u *Uint128) RotateLeft(n uint64) {
	if n&64 != 0 {
		u.lo, u.hi = u.hi, u.lo
	}
	n &= 63
	if n == 0 {
		return
	}
	unbits := 64 - n

	u.lo, u.hi = (u.lo<<n)|(u.hi>>unbits), (u.hi<<n)|(u.lo>>unbits)
}

// ShiftRight shifts u right by n bits.
func (u *Uint128) ShiftRight(n uint64) {
	if n > 127 {
		u.lo, u.hi = 0, 0
		return
	}
	if n >= 64 {
		u.lo, u.hi = u.hi>>(n&63), 0
		return
	}
	unbits := 64 - n

	u.lo, u.hi = (u.lo>>n)|(u.hi<<unbits), (u.hi >> n)
}

// ShiftRightCarry returns both the shifted value and the bits that
// were shifted out. Useful for when you want both x%N and x/N for
// N a power of 2. Only sane if bits <= 64.
func (u *Uint128) ShiftRightCarry(n uint64) (out Uint128, carry uint64) {
	if n > 64 {
		return out, carry
	}
	if n == 64 {
		out.lo, carry = u.hi, u.lo
		return out, carry
	}
	unbits := 64 - n

	out.lo, out.hi, carry = (u.lo>>n)|(u.hi<<unbits), (u.hi >> n), u.lo&((1<<n)-1)
	return out, carry
}

// ShiftLeft rotates u left by n bits.
func (u *Uint128) ShiftLeft(n uint64) {
	if n > 127 {
		u.lo, u.hi = 0, 0
		return
	}
	if n >= 64 {
		u.lo, u.hi = 0, u.lo<<(n&63)
		return
	}
	n &= 63
	if n == 0 {
		return
	}
	unbits := 64 - n

	u.lo, u.hi = (u.lo << n), (u.hi<<n)|(u.lo>>unbits)
}

// Bit returns 1 if the nth bit is set, 0 otherwise.
func (u *Uint128) Bit(n uint64) uint64 {
	if n >= 128 {
		return 0
	}
	if n >= 64 {
		return (u.hi >> (n & 63)) & 1
	}
	return (u.lo >> n) & 1
}

// Inc increments its receiver in place.
func (u *Uint128) Inc() {
	u.lo++
	if u.lo == 0 {
		u.hi++
	}
}
